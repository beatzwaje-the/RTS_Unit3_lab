"""
Mini Project 3.2 - FLAWED bounded-buffer producer-consumer (DEADLOCKS)
======================================================================
Multi-producer, multi-consumer simulation around a bounded shared buffer
protected by an explicit threading.Lock (NOT a queue.Queue, so we control
the acquisition order).

The flawed version DELIBERATELY creates Coffman's four necessary
conditions for deadlock (Section 3.8):

  1. Mutual exclusion       - both buffer_lock and stats_lock are
                              non-shareable mutexes.
  2. Hold and wait          - producers hold buffer_lock while waiting
                              for stats_lock; consumers hold stats_lock
                              while waiting for buffer_lock. A small
                              widening sleep inside each nested region
                              makes the overlap window reliable.
  3. No preemption          - threading.Lock.acquire() cannot be forced
                              away from a holder; the holder must release.
  4. Circular wait          - producers acquire  buffer_lock -> stats_lock
                              consumers acquire  stats_lock  -> buffer_lock
                              => a cycle exists in the resource-allocation
                              graph.

A watchdog thread samples the produced-item count and reports a deadlock
when it stops changing for a configurable stall threshold AND the total
has not yet reached the target. It then prints a lock-wait snapshot so
the deadlock is provable from console output alone.

Usage:
    python3 bounded_buffer_deadlock.py
"""
import threading
import time
import sys

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
N_PRODUCERS = 3
N_CONSUMERS = 3
ITEMS_PER_PRODUCER = 50
BUFFER_CAPACITY = 4
TARGET_TOTAL = N_PRODUCERS * ITEMS_PER_PRODUCER  # 150

WATCHDOG_STALL_S = 1.0       # report deadlock if produced count stalls this long
WATCHDOG_BUDGET_S = 8.0      # hard wall-clock budget for the whole run

# Width of the deadlock window: a small sleep between acquiring the first
# lock and trying the second. Wide enough that the buggy nested-acquisition
# pattern actually overlaps between some producer and some consumer.
DEADLOCK_WINDOW_S = 0.002


# ---------------------------------------------------------------------------
# Shared state
# ---------------------------------------------------------------------------
buffer_lock = threading.Lock()       # guards `the_buffer`
stats_lock = threading.Lock()        # guards `stats`

the_buffer = []                      # the bounded buffer itself
stats = {
    "produced": 0,
    "consumed": 0,
    "total_producer_wait_ms": 0.0,
    "total_consumer_wait_ms": 0.0,
}

# Lock-free progress counters the watchdog can poll WITHOUT acquiring
# stats_lock. In CPython, plain int reads/writes are atomic thanks to the
# GIL, so the watchdog can observe progress even when stats_lock is held
# by a deadlocked consumer.
produced_count = 0
consumed_count = 0

stop_flag = threading.Event()


# ---------------------------------------------------------------------------
# Producers and consumers - ACQUIRE IN OPPOSITE ORDERS (the bug)
# ---------------------------------------------------------------------------
def producer(pid):
    """Acquire buffer_lock FIRST, then stats_lock. With consumers doing
    the opposite, a circular wait can form."""
    global produced_count
    for _ in range(ITEMS_PER_PRODUCER):
        if stop_flag.is_set():
            return

        # ---- producer wait for buffer space (brief buffer_lock holds) ----
        wait_start = time.perf_counter()
        while True:
            with buffer_lock:
                if len(the_buffer) < BUFFER_CAPACITY:
                    break
            if stop_flag.is_set():
                return
            time.sleep(0)
        wait_ms = (time.perf_counter() - wait_start) * 1000.0

        # ---- BUG: hold buffer_lock, then acquire stats_lock ----
        buffer_lock.acquire()
        try:
            the_buffer.append(pid)
            # Widen the deadlock window so a consumer has time to enter its
            # own buggy nested-acquisition region while we still hold
            # buffer_lock.
            time.sleep(DEADLOCK_WINDOW_S)
            # Now acquire stats_lock WHILE HOLDING buffer_lock
            stats_lock.acquire()
            try:
                stats["produced"] += 1
                stats["total_producer_wait_ms"] += wait_ms
                produced_count = stats["produced"]
            finally:
                stats_lock.release()
        finally:
            buffer_lock.release()

        time.sleep(0.0001)  # tiny yield


def consumer(cid):
    """Acquire stats_lock FIRST, then buffer_lock. With producers doing
    the opposite, a circular wait can form."""
    global consumed_count
    while not stop_flag.is_set():
        # Quick exit if we've consumed everything (lock-free read of the
        # mirror counter; safe in CPython thanks to the GIL).
        if consumed_count >= TARGET_TOTAL:
            return

        # ---- consumer wait for available item (brief buffer_lock holds) ----
        wait_start = time.perf_counter()
        while True:
            with buffer_lock:
                has_item = len(the_buffer) > 0
            if has_item:
                break
            if consumed_count >= TARGET_TOTAL:
                return
            if stop_flag.is_set():
                return
            time.sleep(0)
        wait_ms = (time.perf_counter() - wait_start) * 1000.0

        # ---- BUG: hold stats_lock, then acquire buffer_lock ----
        stats_lock.acquire()
        try:
            if stats["consumed"] >= TARGET_TOTAL:
                return
            # Widen the deadlock window so a producer has time to enter its
            # own buggy nested-acquisition region while we still hold
            # stats_lock.
            time.sleep(DEADLOCK_WINDOW_S)
            buffer_lock.acquire()
            try:
                if the_buffer:
                    the_buffer.pop(0)
                stats["consumed"] += 1
                stats["total_consumer_wait_ms"] += wait_ms
                consumed_count = stats["consumed"]
            finally:
                buffer_lock.release()
        finally:
            stats_lock.release()

        time.sleep(0.0001)


# ---------------------------------------------------------------------------
# Watchdog
# ---------------------------------------------------------------------------
def watchdog():
    """Detects when the produced-item count stops changing AND has not yet
    reached the target, then reports a deadlock. Polls the lock-free
    `produced_count` mirror so the watchdog can NEVER itself block on
    stats_lock (which is held by a deadlocked consumer). Prints a lock-wait
    snapshot of every running thread so the deadlock is provable from
    console output alone."""
    last_produced = -1
    last_change_t = time.perf_counter()
    deadline = time.perf_counter() + WATCHDOG_BUDGET_S

    while time.perf_counter() < deadline and not stop_flag.is_set():
        time.sleep(0.1)
        # Lock-free reads of the mirror counters - never block.
        current = produced_count
        consumed_now = consumed_count

        if current != last_produced:
            last_produced = current
            last_change_t = time.perf_counter()
            print(f"[watchdog] produced={current}/{TARGET_TOTAL}, "
                  f"consumed={consumed_now}, progress OK")
        else:
            stalled_for = time.perf_counter() - last_change_t
            if stalled_for >= WATCHDOG_STALL_S and current < TARGET_TOTAL:
                print(f"\n[watchdog] DEADLOCK DETECTED - produced count has "
                      f"stalled at {current}/{TARGET_TOTAL} for "
                      f"{stalled_for:.2f} s")
                print_lock_snapshot()
                stop_flag.set()
                return

    # Final summary if we exited normally (no deadlock)
    if not stop_flag.is_set():
        print(f"\n[watchdog] budget elapsed without deadlock "
              f"(produced={produced_count}/{TARGET_TOTAL}, "
              f"consumed={consumed_count})")


def print_lock_snapshot():
    """Snapshot of lock state and which threads are blocked. We can't
    directly introspect which lock a thread is blocked on in pure-Python,
    so we report the lock state and enumerate the threads that are still
    alive (i.e. blocked somewhere)."""
    print("\n--- LOCK-WAIT SNAPSHOT ---")
    print(f"  buffer_lock held? {buffer_lock.locked()}  "
          f"(held by some producer; that producer is blocked on stats_lock)")
    print(f"  stats_lock  held? {stats_lock.locked()}  "
          f"(held by some consumer; that consumer is blocked on buffer_lock)")
    print(f"  items in buffer: {len(the_buffer)}")
    print(f"  produced so far: {produced_count}/{TARGET_TOTAL}")
    print(f"  consumed so far: {consumed_count}/{TARGET_TOTAL}")
    main_thread = threading.main_thread()
    alive = [t for t in threading.enumerate()
             if t is not main_thread and t.is_alive()]
    print(f"  threads still alive (i.e. blocked): {len(alive)}")
    for t in alive:
        print(f"    - {t.name}")
    print("\n  Circular-wait cycle (per the known buggy acquisition order):")
    print("    Producer: holds buffer_lock -> waits for stats_lock")
    print("    Consumer: holds stats_lock  -> waits for buffer_lock")
    print("    => cycle in the resource-allocation graph (Coffman #4)")
    print("--- END SNAPSHOT ---\n")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    print("=" * 70)
    print("Mini Project 3.2 - FLAWED bounded-buffer (deadlock version)")
    print("=" * 70)
    print(f"Producers: {N_PRODUCERS}, Consumers: {N_CONSUMERS}, "
          f"items/producer: {ITEMS_PER_PRODUCER}, buffer capacity: "
          f"{BUFFER_CAPACITY}")
    print(f"Watchdog stall threshold: {WATCHDOG_STALL_S}s, "
          f"budget: {WATCHDOG_BUDGET_S}s")
    print(f"Deadlock-window widening sleep: "
          f"{DEADLOCK_WINDOW_S*1000:.1f} ms\n")

    threads = []
    for i in range(N_PRODUCERS):
        t = threading.Thread(target=producer, args=(i,),
                             name=f"Producer-{i}", daemon=True)
        t.start()
        threads.append(t)
    for i in range(N_CONSUMERS):
        t = threading.Thread(target=consumer, args=(i,),
                             name=f"Consumer-{i}", daemon=True)
        t.start()
        threads.append(t)

    watchdog_t = threading.Thread(target=watchdog, name="Watchdog",
                                  daemon=True)
    watchdog_t.start()

    budget = WATCHDOG_BUDGET_S + 1.0
    start = time.perf_counter()
    while time.perf_counter() - start < budget:
        if stop_flag.is_set():
            break
        time.sleep(0.1)

    stop_flag.set()
    watchdog_t.join(timeout=2.0)
    for t in threads:
        t.join(timeout=2.0)

    print(f"\nFINAL STATE:")
    print(f"  produced:                {produced_count}/{TARGET_TOTAL}")
    print(f"  consumed:                {consumed_count}/{TARGET_TOTAL}")
    print(f"  items still in buffer:   {len(the_buffer)}")

    if produced_count < TARGET_TOTAL:
        print(f"\nDEADLOCK CONFIRMED: only {produced_count} of "
              f"{TARGET_TOTAL} items were produced.")
    else:
        print("\nNo deadlock observed on this run (re-run for it).")
    sys.exit(0)


if __name__ == "__main__":
    main()

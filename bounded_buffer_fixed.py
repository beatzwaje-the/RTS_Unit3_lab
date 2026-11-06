"""
Mini Project 3.2 - CORRECTED bounded-buffer producer-consumer
==============================================================
Same multi-producer, multi-consumer simulation as the flawed version, but
both producers AND consumers now acquire the two shared locks in ONE
agreed order:  buffer_lock FIRST, then stats_lock.

This eliminates Coffman's circular-wait condition (Section 3.8). Because
no cycle exists in the resource-allocation graph, deadlock is impossible
regardless of how the threads interleave. The watchdog now reports
clean completion of all 150 items.

The corrected version is also instrumented to record, per item:
  - producer wait time for buffer space
  - consumer wait time for an available item
and to report overall throughput.
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

WATCHDOG_STALL_S = 1.5
WATCHDOG_BUDGET_S = 15.0


# ---------------------------------------------------------------------------
# Shared state
# ---------------------------------------------------------------------------
buffer_lock = threading.Lock()
stats_lock = threading.Lock()

the_buffer = []
stats = {
    "produced": 0,
    "consumed": 0,
    "total_producer_wait_ms": 0.0,
    "total_consumer_wait_ms": 0.0,
    "producer_waits": [],     # per-item producer wait times (ms)
    "consumer_waits": [],     # per-item consumer wait times (ms)
}

# Lock-free progress counters for the watchdog
produced_count = 0
consumed_count = 0

stop_flag = threading.Event()


# ---------------------------------------------------------------------------
# Producers and consumers - SAME acquisition order (buffer -> stats)
# ---------------------------------------------------------------------------
def producer(pid):
    """Acquire buffer_lock FIRST, then stats_lock. Same order as the
    consumer, so no cycle can exist in the resource graph."""
    global produced_count
    for _ in range(ITEMS_PER_PRODUCER):
        if stop_flag.is_set():
            return

        # ---- producer wait for buffer space ----
        wait_start = time.perf_counter()
        while True:
            with buffer_lock:
                if len(the_buffer) < BUFFER_CAPACITY:
                    break
            if stop_flag.is_set():
                return
            time.sleep(0)
        wait_ms = (time.perf_counter() - wait_start) * 1000.0

        # ---- CONSISTENT ORDER: buffer_lock -> stats_lock ----
        buffer_lock.acquire()
        try:
            the_buffer.append(pid)
            # Small sleep to mirror the flawed version's timing - proves the
            # fix is structural, not just faster.
            time.sleep(0.002)
            stats_lock.acquire()
            try:
                stats["produced"] += 1
                stats["total_producer_wait_ms"] += wait_ms
                stats["producer_waits"].append(wait_ms)
                produced_count = stats["produced"]
            finally:
                stats_lock.release()
        finally:
            buffer_lock.release()

        time.sleep(0.0001)


def consumer(cid):
    """Acquire buffer_lock FIRST, then stats_lock. SAME order as the
    producer, so no cycle can exist."""
    global consumed_count
    while not stop_flag.is_set():
        if consumed_count >= TARGET_TOTAL:
            return

        # ---- consumer wait for available item ----
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

        # ---- CONSISTENT ORDER: buffer_lock -> stats_lock ----
        buffer_lock.acquire()
        try:
            if not the_buffer:
                buffer_lock.release()
                continue
            the_buffer.pop(0)
            time.sleep(0.002)  # mirror flawed version's timing
            stats_lock.acquire()
            try:
                stats["consumed"] += 1
                stats["total_consumer_wait_ms"] += wait_ms
                stats["consumer_waits"].append(wait_ms)
                consumed_count = stats["consumed"]
            finally:
                stats_lock.release()
        finally:
            buffer_lock.release()

        time.sleep(0.0001)


# ---------------------------------------------------------------------------
# Watchdog
# ---------------------------------------------------------------------------
def watchdog():
    """Polls the lock-free progress counters and reports either clean
    completion or a deadlock. With the lock-ordering fix in place, only
    clean completion should ever occur."""
    last_produced = -1
    last_change_t = time.perf_counter()
    deadline = time.perf_counter() + WATCHDOG_BUDGET_S

    while time.perf_counter() < deadline and not stop_flag.is_set():
        time.sleep(0.1)
        current = produced_count
        consumed_now = consumed_count

        if current != last_produced:
            last_produced = current
            last_change_t = time.perf_counter()

        stalled_for = time.perf_counter() - last_change_t
        if stalled_for >= WATCHDOG_STALL_S and current < TARGET_TOTAL:
            print(f"\n[watchdog] DEADLOCK DETECTED - produced count has "
                  f"stalled at {current}/{TARGET_TOTAL} for "
                  f"{stalled_for:.2f} s")
            print(f"  buffer_lock held? {buffer_lock.locked()}")
            print(f"  stats_lock  held? {stats_lock.locked()}")
            stop_flag.set()
            return

        # Clean completion check
        if current >= TARGET_TOTAL and consumed_now >= TARGET_TOTAL:
            print(f"\n[watchdog] CLEAN COMPLETION - all {TARGET_TOTAL} "
                  f"items produced and consumed. No deadlock.")
            stop_flag.set()
            return

    if not stop_flag.is_set():
        print(f"\n[watchdog] budget elapsed (produced={produced_count}/"
              f"{TARGET_TOTAL}, consumed={consumed_count}/{TARGET_TOTAL})")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    print("=" * 70)
    print("Mini Project 3.2 - CORRECTED bounded-buffer (lock ordering)")
    print("=" * 70)
    print(f"Producers: {N_PRODUCERS}, Consumers: {N_CONSUMERS}, "
          f"items/producer: {ITEMS_PER_PRODUCER}, buffer capacity: "
          f"{BUFFER_CAPACITY}")
    print(f"Watchdog stall threshold: {WATCHDOG_STALL_S}s, "
          f"budget: {WATCHDOG_BUDGET_S}s\n")

    t0 = time.perf_counter()

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

    elapsed = time.perf_counter() - t0
    stop_flag.set()
    watchdog_t.join(timeout=2.0)
    for t in threads:
        t.join(timeout=2.0)

    print(f"\nFINAL STATE:")
    print(f"  produced:                {produced_count}/{TARGET_TOTAL}")
    print(f"  consumed:                {consumed_count}/{TARGET_TOTAL}")
    print(f"  items still in buffer:   {len(the_buffer)}")
    print(f"  elapsed wall-clock:      {elapsed*1000:.2f} ms")

    if produced_count > 0:
        throughput = produced_count / elapsed  # elapsed is already in seconds
        print(f"  throughput:              {throughput:.1f} items/sec")

    if stats["producer_waits"]:
        pw = stats["producer_waits"]
        cw = stats["consumer_waits"]
        print(f"\n  Producer wait (ms):  "
              f"min={min(pw):.3f}, max={max(pw):.3f}, "
              f"mean={sum(pw)/len(pw):.3f}, n={len(pw)}")
        if cw:
            print(f"  Consumer wait (ms):  "
                  f"min={min(cw):.3f}, max={max(cw):.3f}, "
                  f"mean={sum(cw)/len(cw):.3f}, n={len(cw)}")

    if produced_count == TARGET_TOTAL and consumed_count == TARGET_TOTAL:
        print("\nSUCCESS: all items produced and consumed, no deadlock.")
    else:
        print("\nPARTIAL: some items were not produced/consumed.")
    sys.exit(0)


if __name__ == "__main__":
    main()

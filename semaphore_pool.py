"""
Mini Project 3.1 - Counting Semaphore Resource Pool
====================================================
Models a fixed-size resource pool (Sections 3.4 and 3.6) using
threading.Semaphore(3) - three resource blocks. Eight task threads
attempt to acquire a block, hold it for a random short interval, then
release it. Each task records how long it waited before acquiring.

Expected behaviour: at most three tasks hold the resource simultaneously,
and the remaining five show measurable wait time, consistent with
counting-semaphore blocking.
"""
import threading
import time
import random
import statistics

# Track simultaneous holders in real time so we can verify the
# semaphore's invariant: at most `count` holders at any instant.
holders_lock = threading.Lock()
current_holders = 0
peak_holders = 0


def task(task_id, sem, hold_min, hold_max, wait_log):
    """Acquire the semaphore, record wait time, hold for a random interval,
    then release."""
    global current_holders, peak_holders
    start_wait = time.perf_counter()
    sem.acquire()
    waited = time.perf_counter() - start_wait
    wait_log.append((task_id, waited * 1000.0))  # ms

    with holders_lock:
        current_holders += 1
        if current_holders > peak_holders:
            peak_holders = current_holders

    hold = random.uniform(hold_min, hold_max)
    time.sleep(hold)

    with holders_lock:
        current_holders -= 1
    sem.release()


def run_pool(sem_count, n_tasks=8, hold_min=0.010, hold_max=0.030):
    """Run n_tasks against a Semaphore(sem_count)."""
    global current_holders, peak_holders
    current_holders = 0
    peak_holders = 0
    sem = threading.Semaphore(sem_count)
    wait_log = []

    threads = [
        threading.Thread(target=task,
                         args=(i, sem, hold_min, hold_max, wait_log))
        for i in range(n_tasks)
    ]
    for t in threads:
        t.start()
        # Stagger launches slightly so some contention actually develops.
        time.sleep(0.002)
    for t in threads:
        t.join()

    waits = [w for _, w in wait_log]
    immediate = sum(1 for w in waits if w < 1.0)
    blocked = sum(1 for w in waits if w >= 1.0)

    print(f"\n--- Semaphore({sem_count}), {n_tasks} tasks ---")
    print(f"Peak simultaneous holders: {peak_holders} "
          f"(invariant: <= {sem_count} -> "
          f"{'OK' if peak_holders <= sem_count else 'VIOLATED'})")
    print(f"Tasks acquired immediately (<1 ms): {immediate}")
    print(f"Tasks with measurable wait (>=1 ms): {blocked}")
    print(f"Wait time min:    {min(waits):.3f} ms")
    print(f"Wait time max:    {max(waits):.3f} ms")
    print(f"Wait time mean:   {statistics.mean(waits):.3f} ms")
    if len(waits) > 1:
        print(f"Wait time stdev:  {statistics.stdev(waits):.3f} ms")
    # Per-task wait times so we can see which tasks blocked.
    for tid, w in sorted(wait_log):
        print(f"  task {tid}: waited {w:7.3f} ms")


def main():
    print("=" * 60)
    print("semaphore_pool.py  - Semaphore(3) resource pool")
    print("=" * 60)
    run_pool(sem_count=3)


if __name__ == "__main__":
    main()

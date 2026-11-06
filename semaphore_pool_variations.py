"""
Lab Task 2 - Semaphore pool with counts 1 and 6
================================================
Repeats the semaphore_pool.py experiment with the semaphore count set to
1 (a strict mutex - only one holder at a time) and to 6 (only two tasks
must wait at all, since 8 - 6 = 2). We compare contention and wait times
against the baseline Semaphore(3) result, relating the change to Section
3.4's counting-semaphore discussion.
"""
import threading
import time
import random
import statistics

holders_lock = threading.Lock()
current_holders = 0
peak_holders = 0


def task(task_id, sem, hold_min, hold_max, wait_log):
    global current_holders, peak_holders
    start_wait = time.perf_counter()
    sem.acquire()
    waited = time.perf_counter() - start_wait
    wait_log.append((task_id, waited * 1000.0))

    with holders_lock:
        current_holders += 1
        if current_holders > peak_holders:
            peak_holders = current_holders

    time.sleep(random.uniform(hold_min, hold_max))

    with holders_lock:
        current_holders -= 1
    sem.release()


def run_pool(sem_count, n_tasks=8, hold_min=0.010, hold_max=0.030):
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
        time.sleep(0.002)
    for t in threads:
        t.join()

    waits = [w for _, w in wait_log]
    immediate = sum(1 for w in waits if w < 1.0)
    blocked = sum(1 for w in waits if w >= 1.0)

    print(f"\n--- Semaphore({sem_count}), {n_tasks} tasks ---")
    print(f"Peak simultaneous holders: {peak_holders} "
          f"(invariant <= {sem_count}: "
          f"{'OK' if peak_holders <= sem_count else 'VIOLATED'})")
    print(f"Tasks acquired immediately (<1 ms): {immediate}")
    print(f"Tasks with measurable wait (>=1 ms): {blocked}")
    print(f"Wait time min:  {min(waits):.3f} ms")
    print(f"Wait time max:  {max(waits):.3f} ms")
    print(f"Wait time mean: {statistics.mean(waits):.3f} ms")
    if len(waits) > 1:
        print(f"Wait time stdev: {statistics.stdev(waits):.3f} ms")


def main():
    print("=" * 60)
    print("Lab Task 2 - Semaphore count sweep (1, 3, 6)")
    print("=" * 60)
    for c in (1, 3, 6):
        run_pool(sem_count=c)


if __name__ == "__main__":
    main()

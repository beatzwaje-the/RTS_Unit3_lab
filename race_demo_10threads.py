"""
Lab Task 1 - Race condition with TEN threads instead of five
=============================================================
Repeats the race_demo.py experiment with 10 threads x 1000 iterations.
We compare the magnitude of the lost-update error against the 5-thread
case to determine whether the error grows, shrinks, or stays roughly the
same when contention increases.
"""
import threading
import time

shared_counter = 0


def increment_unsafe(iterations):
    global shared_counter
    for _ in range(iterations):
        current = shared_counter
        time.sleep(0.0001)
        shared_counter = current + 1


def main():
    global shared_counter
    num_threads = 10
    iterations = 1000
    expected = num_threads * iterations  # 10000

    print("=" * 60)
    print("Lab Task 1 - race_demo with 10 threads (UNSAFE)")
    print("=" * 60)
    print(f"Threads:              {num_threads}")
    print(f"Iterations/thread:    {iterations}")
    print(f"Expected final value: {expected}")
    print()

    for run in range(1, 4):
        shared_counter = 0
        threads = [
            threading.Thread(target=increment_unsafe, args=(iterations,))
            for _ in range(num_threads)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        lost = expected - shared_counter
        pct = 100.0 * lost / expected
        print(f"Run {run}: final = {shared_counter:6d}  "
              f"lost = {lost:5d}  ({pct:5.2f}% lost)")


if __name__ == "__main__":
    main()

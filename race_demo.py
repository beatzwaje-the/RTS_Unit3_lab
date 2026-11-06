"""
Mini Project 3.1 - Race Condition Demonstration (UNSAFE)
=========================================================
Reproduces the lost-update race condition from Section 3.8.

Five threads each perform 1000 read-increment-write operations on a shared
counter with no synchronization. A small sleep between the read and the
write widens the interleaving window so the race manifests reliably.

Run the script three times; the final counter is usually below 5000 and
varies between runs - the textbook signature of a non-atomic
read-modify-write race.
"""
import threading
import time

# Module-scope shared state - NO lock guards it.
shared_counter = 0


def increment_unsafe(iterations):
    """Non-atomic read-increment-write. The sleep between read and write
    widens the interleaving window so two threads routinely read the same
    value, both add one, and one update is lost."""
    global shared_counter
    for _ in range(iterations):
        current = shared_counter          # READ
        time.sleep(0.0001)                # widen window (releases GIL)
        shared_counter = current + 1      # WRITE (stale)


def main():
    global shared_counter
    num_threads = 5
    iterations = 1000
    expected = num_threads * iterations  # 5000

    print("=" * 60)
    print("race_demo.py  (UNSAFE - no lock)")
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
        print(f"Run {run}: final = {shared_counter:5d}  "
              f"lost updates = {lost:5d}")


if __name__ == "__main__":
    main()

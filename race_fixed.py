"""
Mini Project 3.1 - Race Condition FIXED with threading.Lock
============================================================
Same scenario as race_demo.py but the critical section is now protected
by a mutex-like threading.Lock, mirroring Section 3.4's mutual-exclusion
primitive. The read-increment-write becomes atomic with respect to other
lock-holders, so the final counter is exactly 5000 on every run.
"""
import threading
import time

shared_counter = 0
counter_lock = threading.Lock()  # mutex-style guard


def increment_safe(iterations):
    """Critical section wrapped in `with lock:` - only one thread may be
    inside at a time, so the read-modify-write is atomic."""
    global shared_counter
    for _ in range(iterations):
        with counter_lock:
            current = shared_counter
            time.sleep(0.0001)        # same widening sleep, now safe
            shared_counter = current + 1


def main():
    global shared_counter
    num_threads = 5
    iterations = 1000
    expected = num_threads * iterations  # 5000

    print("=" * 60)
    print("race_fixed.py  (SAFE - threading.Lock)")
    print("=" * 60)
    print(f"Threads:              {num_threads}")
    print(f"Iterations/thread:    {iterations}")
    print(f"Expected final value: {expected}")
    print()

    for run in range(1, 4):
        shared_counter = 0
        threads = [
            threading.Thread(target=increment_safe, args=(iterations,))
            for _ in range(num_threads)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        ok = (shared_counter == expected)
        print(f"Run {run}: final = {shared_counter:5d}  "
              f"{'OK (== expected)' if ok else 'MISMATCH'}")


if __name__ == "__main__":
    main()

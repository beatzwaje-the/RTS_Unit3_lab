"""
Lab Task 3 - Livelock demonstration (deterministic, barrier-synchronised)
=========================================================================
Two threads each try to acquire two shared locks in OPPOSITE orders, then
back off and retry. To make the livelock deterministic (rather than
dependent on GIL scheduling luck) we use three per-round Barriers so that:

  Phase 1: both threads try to acquire their PRIMARY lock.
  Phase 2: both threads try to acquire their SECONDARY lock (neither has
           released anything yet, so both attempts fail when both hold
           their primary).
  Phase 3: both threads release whatever they hold and loop back.

T1's primary is A, secondary is B. T2's primary is B, secondary is A.
Each round, both threads successfully grab their primary (different locks,
no conflict), then both fail to grab their secondary (held by the other),
then both release, then both retry. This is the textbook livelock of
Section 3.8: tasks change state repeatedly (high retry count) without ever
making forward progress (zero successes).
"""
import threading
import time

lock_a = threading.Lock()
lock_b = threading.Lock()

# Three reusable barriers, one per phase, so the two threads stay in
# lockstep every round.
b1 = threading.Barrier(2, timeout=1.0)
b2 = threading.Barrier(2, timeout=1.0)
b3 = threading.Barrier(2, timeout=1.0)

retries = {"T1": 0, "T2": 0}
successes = {"T1": 0, "T2": 0}
stop_flag = {"stop": False}


def philosopher(name, primary, secondary):
    """One thread that tries primary -> secondary each round, releasing
    primary on conflict. Because both threads use opposite primaries,
    every round they collide and back off."""
    while not stop_flag["stop"] and (successes["T1"] + successes["T2"] == 0):
        retries[name] += 1

        # Phase 1: try to grab primary
        got_primary = primary.acquire(blocking=False)
        try:
            b1.wait()
        except threading.BrokenBarrierError:
            return

        # Phase 2: try to grab secondary (other thread still holds its
        # primary, so this fails when both hold their primary).
        got_secondary = False
        if got_primary:
            got_secondary = secondary.acquire(blocking=False)
        try:
            b2.wait()
        except threading.BrokenBarrierError:
            if got_secondary:
                secondary.release()
            if got_primary:
                primary.release()
            return

        # Phase 3: react and release
        if got_secondary:
            successes[name] += 1
            secondary.release()
            primary.release()
            stop_flag["stop"] = True
            try:
                b3.wait()
            except threading.BrokenBarrierError:
                pass
            return
        elif got_primary:
            primary.release()
        try:
            b3.wait()
        except threading.BrokenBarrierError:
            return
        time.sleep(0)  # yield


def main():
    print("=" * 60)
    print("Lab Task 3 - Livelock demo (opposite-order lock acquisition)")
    print("=" * 60)

    t1 = threading.Thread(target=philosopher, args=("T1", lock_a, lock_b))
    t2 = threading.Thread(target=philosopher, args=("T2", lock_b, lock_a))
    t1.start()
    t2.start()

    # Let them spin for a bounded wall-clock budget.
    budget = 2.0
    time.sleep(budget)
    stop_flag["stop"] = True
    # Break the barriers so threads waiting on them can exit.
    b1.abort()
    b2.abort()
    b3.abort()

    t1.join(timeout=2.0)
    t2.join(timeout=2.0)

    print(f"\nAfter ~{budget:.1f} s of running:")
    print(f"  T1 retries:   {retries['T1']:8d}   T1 successes: {successes['T1']}")
    print(f"  T2 retries:   {retries['T2']:8d}   T2 successes: {successes['T2']}")
    total_success = successes["T1"] + successes["T2"]
    total_retries = retries["T1"] + retries["T2"]
    print(f"\nTotal retries:   {total_retries}")
    print(f"Total successes: {total_success}")

    if total_success == 0 and total_retries > 100:
        print("\nVERDICT: livelock - both threads stayed busy (high retry "
              "count) but neither made any forward progress (zero "
              "successes). This matches Section 3.8's livelock definition: "
              "tasks change state repeatedly without ever completing.")
    elif total_success > 0:
        print("\nVERDICT: at least one thread made progress - no livelock.")


if __name__ == "__main__":
    main()

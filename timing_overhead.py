"""
Mini Project 3.1 - Synchronization / Dispatch-Latency Overhead
===============================================================
Measures the overhead of starting a thread that signals a threading.Event
and waiting on that event from the main thread. This stands in for the
context-switch / interrupt-response latency of Section 3.5: the time
between the moment the main thread asks for a context switch (start of
worker thread) and the moment it observes the side effect (event set).

The maximum observed value is labeled as the worst-case observed dispatch
latency, illustrating why RTOS designers reason about worst case rather
than average case (Section 3.1).
"""
import threading
import time
import statistics


def measure_one():
    """Spawn a thread whose only job is to set an Event; main thread blocks
    on event.wait(). Returns the elapsed wall-clock time in milliseconds."""
    ev = threading.Event()
    start = time.perf_counter()

    def worker():
        ev.set()

    t = threading.Thread(target=worker)
    t.start()
    ev.wait()
    elapsed = (time.perf_counter() - start) * 1000.0  # ms
    t.join()
    return elapsed


def percentile(data, p):
    """Linear-interpolation percentile."""
    if not data:
        return 0.0
    s = sorted(data)
    k = (len(s) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(s) - 1)
    return s[f] + (s[c] - s[f]) * (k - f)


def main():
    n = 200
    samples = [measure_one() for _ in range(n)]

    print("=" * 60)
    print(f"timing_overhead.py  - {n} iterations")
    print("=" * 60)
    print(f"Min:               {min(samples):.4f} ms")
    print(f"Mean:              {statistics.mean(samples):.4f} ms")
    print(f"Max:               {max(samples):.4f} ms"
          "   <- worst-case observed dispatch latency")
    print(f"95th percentile:   {percentile(samples, 95):.4f} ms")
    print(f"Stdev:             {statistics.stdev(samples):.4f} ms")

    # Histogram-ish bucket view so the long tail is visible.
    print("\nDistribution:")
    buckets = [0, 0.05, 0.10, 0.20, 0.50, 1.00, 2.00, 5.00, float("inf")]
    labels = ["<0.05", "<0.10", "<0.20", "<0.50", "<1.00",
              "<2.00", "<5.00", ">=5.00"]
    counts = [0] * len(labels)
    for s in samples:
        for i in range(len(buckets) - 1):
            if buckets[i] <= s < buckets[i+1]:
                counts[i] += 1
                break
    for lab, cnt in zip(labels, counts):
        bar = "#" * cnt
        print(f"  {lab:>7} ms: {cnt:3d}  {bar}")


if __name__ == "__main__":
    main()

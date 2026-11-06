"""
Mini Project 3.1 - Bounded Message Queue Simulator
===================================================
Models an RTOS message queue (Section 3.4) with a bounded queue.Queue.
One producer thread puts 20 timestamped items into a Queue(maxsize=5);
one consumer thread pulls them out and measures the send->receipt latency.

Two scenarios are run back-to-back:
  (A) baseline - consumer pulls as fast as it can.
  (B) slowed   - consumer adds a small sleep in its loop, demonstrating
                 that both per-item latency and producer blocking grow.

Echoes Section 3.5's response-latency discussion: a slow consumer pushes
the latency of every queued item up and forces the producer to block.
"""
import threading
import queue
import time
import statistics


def producer_task(q, n_items, blocking_log):
    """Put n_items timestamped items into the bounded queue. put() blocks
    automatically when the queue is full, mirroring an RTOS message queue
    that suspends the producing task on a full mailbox."""
    for i in range(n_items):
        send_ts = time.perf_counter()
        item = {"id": i, "send_ts": send_ts}
        try:
            q.put(item, timeout=2.0)
        except queue.Full:
            blocking_log.append((i, "TIMEOUT"))
            continue
        # Detect whether the put blocked by re-reading the clock.
        block_dur = time.perf_counter() - send_ts
        if block_dur > 0.001:
            blocking_log.append((i, f"{block_dur*1000:.2f} ms"))


def consumer_task(q, n_items, latencies, slow=False):
    """Pull n_items items from the queue and record send->receipt latency."""
    for _ in range(n_items):
        item = q.get(timeout=5.0)
        recv_ts = time.perf_counter()
        latencies.append((recv_ts - item["send_ts"]) * 1000.0)  # ms
        if slow:
            time.sleep(0.005)  # 5 ms sleep simulates a slow consumer ISR/task


def run_scenario(label, n_items=20, maxsize=5, slow=False):
    q = queue.Queue(maxsize=maxsize)
    latencies = []
    blocking_log = []

    t0 = time.perf_counter()
    cons = threading.Thread(target=consumer_task,
                            args=(q, n_items, latencies, slow))
    prod = threading.Thread(target=producer_task,
                            args=(q, n_items, blocking_log))
    cons.start()
    prod.start()
    prod.join()
    cons.join()
    elapsed = time.perf_counter() - t0

    print(f"\n--- {label} (maxsize={maxsize}, items={n_items}, "
          f"slow_consumer={slow}) ---")
    print(f"Total elapsed:        {elapsed*1000:.2f} ms")
    print(f"Latency min:          {min(latencies):.3f} ms")
    print(f"Latency max:          {max(latencies):.3f} ms")
    print(f"Latency mean:         {statistics.mean(latencies):.3f} ms")
    print(f"Latency stdev:        {statistics.stdev(latencies):.3f} ms")
    print(f"Producer block events: {len(blocking_log)}")
    if blocking_log:
        print("  Sample (id, block_dur): "
              f"{blocking_log[:3]} ... {blocking_log[-1]}")


def main():
    print("=" * 60)
    print("queue_sim.py  - bounded Queue(5) message simulator")
    print("=" * 60)
    run_scenario("A. baseline (fast consumer)")
    run_scenario("B. slowed consumer (5 ms sleep per item)", slow=True)


if __name__ == "__main__":
    main()

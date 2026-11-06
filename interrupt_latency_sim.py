"""
Mini Project 3.3 - Priority-Based Interrupt Latency Simulator
==============================================================
Simulates a periodic interrupt source set served by multiple ISRs of
differing priority, following the top-half / bottom-half pattern of
Section 3.5.

Architecture
------------
* Each interrupt source is a THREAD that sleeps for its period, then
  fires: it records an assertion timestamp, busy-waits for its
  top-half duration (holding the GIL, so the CPU worker cannot run -
  this simulates the top-half preempting any executing bottom half),
  and enqueues a bottom-half task tagged with priority and the
  original assertion timestamp into a heap (heapq).
* One CPU-WORKER thread runs the highest-priority ready bottom-half
  task in short slices (1 ms). After each slice it re-checks the
  heap: if a higher-priority task has arrived (enqueued by a top half
  that ran during the slice), the current task is re-queued with its
  remaining time and the higher-priority task is dispatched - this is
  priority-preemptive scheduling of bottom halves, with mid-task
  preemption.
* Per occurrence we log interrupt latency (assertion -> top-half
  start), top-half duration, and response latency (assertion ->
  bottom-half completion). End-of-run we report min/mean/max/p95
  response latency per source.

Three configurations are run:
  A. Baseline: all sources have short top halves (well-behaved).
  B. Long-top-half: the HIGH-priority source violates Section 3.5's
     short-top-half guidance with a 20 ms top half.
  C. Arrival-rate sweep: HIGH source's period is swept across
     [20, 50, 100, 200] ms; we observe the effect on LOW's
     worst-case response latency.

A timeline of a representative window is recorded for visualization.
"""
import threading
import time
import heapq
import statistics
import sys
import os
import json
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple

# Optional matplotlib for the timeline plot
try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    import matplotlib.font_manager as fm
    HAS_MPL = True
except ImportError:
    HAS_MPL = False


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SLICE_MS = 1.0          # bottom-half slice size - preemption granularity
GIL_INTERVAL_S = 0.001  # 1 ms - makes GIL scheduling match our slice size


@dataclass
class SourceConfig:
    name: str
    period_ms: float
    top_half_ms: float
    bottom_half_ms: float
    priority: int        # lower number = higher priority (RTOS convention)


@dataclass
class SourceStats:
    """Per-source statistics accumulated during a run."""
    interrupt_latencies: List[float] = field(default_factory=list)   # ms
    top_half_durations: List[float] = field(default_factory=list)    # ms
    response_latencies: List[float] = field(default_factory=list)    # ms
    bh_preemptions: int = 0      # times a bh slice was re-queued due to a
                                  # higher-priority arrival


# ---------------------------------------------------------------------------
# Simulation state (reset for each run)
# ---------------------------------------------------------------------------
class SimState:
    """Holds all mutable state for one simulation run. Created fresh per
    run so multiple runs don't interfere."""
    def __init__(self):
        self.bh_queue: List[Tuple] = []          # heap of (priority, seq, task)
        self.queue_lock = threading.Lock()
        self.queue_cond = threading.Condition(self.queue_lock)
        self.stats_lock = threading.Lock()
        self.stats: Dict[str, SourceStats] = {}
        self.timeline: List[Tuple] = []          # (ts, event, source, detail)
        self.timeline_lock = threading.Lock()
        self.seq_counter = [0]
        self.stop_flag = threading.Event()
        self.run_start_ts = 0.0
        # The CPU lock: whomever holds it "owns the CPU". A top half
        # acquires it for its entire duration; the CPU worker acquires it
        # for one bottom-half slice at a time. This makes top halves
        # truly preempt the CPU worker (no GIL time-slicing leakage).
        self.cpu_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Helper: busy-wait (holds the GIL, simulating exclusive CPU use)
# ---------------------------------------------------------------------------
def busy_wait_ms(duration_ms: float):
    """Busy-wait for the given duration. Holds the GIL so other threads
    cannot run - this is how we simulate a top half preempting the CPU
    worker."""
    end = time.perf_counter() + duration_ms / 1000.0
    while time.perf_counter() < end:
        pass


def percentile(data: List[float], p: float) -> float:
    if not data:
        return 0.0
    s = sorted(data)
    k = (len(s) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(s) - 1)
    return s[f] + (s[c] - s[f]) * (k - f)


# ---------------------------------------------------------------------------
# Interrupt source thread
# ---------------------------------------------------------------------------
def interrupt_source(cfg: SourceConfig, state: SimState, run_duration_s: float):
    """Periodic interrupt source. Sleeps for its period, fires, runs its
    top half (holding the cpu_lock for the entire duration so the CPU
    worker cannot run), then enqueues a bottom-half task into the priority
    heap.

    Interrupt latency is measured from the SCHEDULED fire time to the
    actual moment the top-half acquires the cpu_lock. The gap is the time
    the thread spent waiting for the CPU (because the CPU worker or
    another top half held it) - the Python analogue of interrupt latency
    in a real RTOS."""
    end_time = state.run_start_ts + run_duration_s
    # Schedule the first fire one period from now
    next_fire = time.perf_counter() + cfg.period_ms / 1000.0

    while next_fire < end_time and not state.stop_flag.is_set():
        # Sleep until the scheduled fire time
        sleep_duration = next_fire - time.perf_counter()
        if sleep_duration > 0:
            time.sleep(sleep_duration)
        if state.stop_flag.is_set():
            return

        # The thread has now woken up. Try to acquire the CPU lock - this
        # is the preemption point. If the CPU worker holds the lock (running
        # a bottom-half slice), we wait; the wait time is the interrupt
        # latency. With 1 ms slices the worst-case wait is ~1 ms.
        acquire_start = time.perf_counter()
        state.cpu_lock.acquire()
        actual_start = time.perf_counter()

        assertion_ts = next_fire  # logical assertion timestamp
        interrupt_latency_ms = (actual_start - next_fire) * 1000.0
        top_half_start = actual_start

        with state.timeline_lock:
            state.timeline.append((assertion_ts, "assert", cfg.name, ""))
            state.timeline.append((top_half_start, "th_start", cfg.name, ""))

        # ---- TOP HALF ----
        # We hold the cpu_lock for the entire top-half duration. The CPU
        # worker cannot run during this time - this is the preemption.
        busy_wait_ms(cfg.top_half_ms)

        top_half_end = time.perf_counter()
        top_half_actual_ms = (top_half_end - top_half_start) * 1000.0

        with state.timeline_lock:
            state.timeline.append((top_half_end, "th_end", cfg.name,
                                   f"{top_half_actual_ms:.2f}ms"))

        # Release the CPU lock - CPU worker can now run
        state.cpu_lock.release()

        # ---- ENQUEUE BOTTOM HALF ----
        # Note: assertion_ts is the SCHEDULED fire time, so response
        # latency will include any interrupt latency the source suffered.
        task = {
            "source": cfg.name,
            "priority": cfg.priority,
            "assertion_ts": assertion_ts,
            "remaining_ms": cfg.bottom_half_ms,
            "total_bh_ms": cfg.bottom_half_ms,
            "bh_seq": state.seq_counter[0],
        }
        with state.queue_cond:
            heapq.heappush(state.bh_queue,
                           (cfg.priority, state.seq_counter[0], task))
            state.seq_counter[0] += 1
            state.queue_cond.notify()

        # Record stats
        with state.stats_lock:
            if cfg.name in state.stats:
                state.stats[cfg.name].interrupt_latencies.append(
                    interrupt_latency_ms)
                state.stats[cfg.name].top_half_durations.append(
                    top_half_actual_ms)

        # Schedule next fire
        next_fire += cfg.period_ms / 1000.0


# ---------------------------------------------------------------------------
# CPU worker thread (priority-preemptive bottom-half scheduler)
# ---------------------------------------------------------------------------
def cpu_worker(state: SimState, run_duration_s: float):
    """Single CPU worker. Pops the highest-priority bottom-half task from
    the heap, acquires the cpu_lock for one slice, runs the slice, then
    releases the lock and re-checks the queue. If a higher-priority task
    has arrived (enqueued by a top half that preempted us mid-slice),
    the current task is re-queued with its remaining time and the
    higher-priority task is dispatched instead.

    The cpu_lock is the preemption mechanism: a top half that fires
    while we're holding it waits until we release it (end of slice); a
    top half that fires while we DON'T hold it (between slices) acquires
    it immediately and we're blocked until the top half ends."""
    end_time = state.run_start_ts + run_duration_s

    while time.perf_counter() < end_time and not state.stop_flag.is_set():
        with state.queue_cond:
            while not state.bh_queue and time.perf_counter() < end_time:
                state.queue_cond.wait(timeout=0.001)
                if state.stop_flag.is_set():
                    return
            if not state.bh_queue:
                continue
            # Pop the highest-priority (lowest priority number) task
            priority, seq, task = heapq.heappop(state.bh_queue)

        # Run a slice of this bottom-half task. Acquire the cpu_lock so
        # that a top half firing during this slice is forced to wait
        # (queued behind the lock) until the slice ends.
        slice_ms = min(SLICE_MS, task["remaining_ms"])
        bh_slice_start = time.perf_counter()

        with state.timeline_lock:
            state.timeline.append((bh_slice_start, "bh_start",
                                   task["source"],
                                   f"rem={task['remaining_ms']:.2f}ms"))

        state.cpu_lock.acquire()
        try:
            busy_wait_ms(slice_ms)
        finally:
            state.cpu_lock.release()

        bh_slice_end = time.perf_counter()
        actual_slice_ms = (bh_slice_end - bh_slice_start) * 1000.0
        task["remaining_ms"] -= actual_slice_ms

        with state.timeline_lock:
            state.timeline.append((bh_slice_end, "bh_slice_end",
                                   task["source"],
                                   f"ran={actual_slice_ms:.2f}ms"))

        if task["remaining_ms"] <= 0.2:  # completion threshold (0.2 ms slack)
            # Bottom half completed - record response latency
            response_ts = time.perf_counter()
            response_latency_ms = (response_ts - task["assertion_ts"]) * 1000.0
            with state.stats_lock:
                if task["source"] in state.stats:
                    state.stats[task["source"]].response_latencies.append(
                        response_latency_ms)
            with state.timeline_lock:
                state.timeline.append((response_ts, "bh_complete",
                                       task["source"],
                                       f"RL={response_latency_ms:.2f}ms"))
        else:
            # Re-queue the remainder. The next loop iteration will pop the
            # highest-priority task - which may now be a different (higher-
            # priority) task that arrived during our slice. That is the
            # priority-preemptive mid-task preemption.
            with state.queue_cond:
                heapq.heappush(state.bh_queue,
                               (task["priority"],
                                state.seq_counter[0], task))
                state.seq_counter[0] += 1


# ---------------------------------------------------------------------------
# Run one simulation
# ---------------------------------------------------------------------------
def run_simulation(sources: List[SourceConfig], duration_s: float,
                   label: str, record_timeline: bool = False,
                   timeline_suffix: str = "") -> Dict:
    """Run one simulation with the given sources. Returns a dict of
    per-source statistics."""
    state = SimState()
    state.stats = {s.name: SourceStats() for s in sources}

    print(f"\n{'='*78}")
    print(f"Configuration: {label}")
    print(f"Duration: {duration_s:.1f} s, slice: {SLICE_MS} ms")
    print(f"{'-'*78}")
    print(f"{'Source':>8} | {'Period':>7} | {'TopHalf':>7} | "
          f"{'BotHalf':>7} | {'Priority':>8}")
    print(f"{'-'*78}")
    for s in sources:
        print(f"{s.name:>8} | {s.period_ms:>7.1f} | {s.top_half_ms:>7.2f} | "
              f"{s.bottom_half_ms:>7.1f} | {s.priority:>8}")
    print(f"{'='*78}")
    sys.stdout.flush()

    # Set GIL switch interval to match our slice size for responsive
    # preemption.
    old_interval = sys.getswitchinterval()
    sys.setswitchinterval(GIL_INTERVAL_S)

    try:
        state.run_start_ts = time.perf_counter()

        # Start interrupt source threads
        threads = []
        for s in sources:
            t = threading.Thread(target=interrupt_source,
                                 args=(s, state, duration_s),
                                 name=f"ISR-{s.name}", daemon=True)
            t.start()
            threads.append(t)

        # Start CPU worker
        worker = threading.Thread(target=cpu_worker,
                                  args=(state, duration_s),
                                  name="CPU-Worker", daemon=True)
        worker.start()
        threads.append(worker)

        # Wait for the run duration
        time.sleep(duration_s + 0.5)
        state.stop_flag.set()

        for t in threads:
            t.join(timeout=2.0)
    finally:
        sys.setswitchinterval(old_interval)

    # Compute and print stats
    print(f"\nResults for: {label}")
    print(f"{'Source':>8} | {'n':>4} | {'IL min':>7} | {'IL mean':>7} | "
          f"{'IL max':>7} | {'IL p95':>7} || {'RL min':>7} | {'RL mean':>7} | "
          f"{'RL max':>7} | {'RL p95':>7}")
    print("-" * 110)

    results = {}
    for s in sources:
        st = state.stats[s.name]
        il = st.interrupt_latencies
        rl = st.response_latencies
        r = {
            "count": len(il),
            "il_min": min(il) if il else 0,
            "il_mean": statistics.mean(il) if il else 0,
            "il_max": max(il) if il else 0,
            "il_p95": percentile(il, 95) if il else 0,
            "rl_min": min(rl) if rl else 0,
            "rl_mean": statistics.mean(rl) if rl else 0,
            "rl_max": max(rl) if rl else 0,
            "rl_p95": percentile(rl, 95) if rl else 0,
            "rl_count": len(rl),
        }
        results[s.name] = r
        print(f"{s.name:>8} | {r['count']:>4} | {r['il_min']:7.3f} | "
              f"{r['il_mean']:7.3f} | {r['il_max']:7.3f} | "
              f"{r['il_p95']:7.3f} || {r['rl_min']:7.3f} | "
              f"{r['rl_mean']:7.3f} | {r['rl_max']:7.3f} | "
              f"{r['rl_p95']:7.3f}")

    if record_timeline and HAS_MPL:
        plot_timeline(state, label, sources, timeline_suffix)

    return results


# ---------------------------------------------------------------------------
# Timeline plot
# ---------------------------------------------------------------------------
def plot_timeline(state: SimState, label: str,
                  sources: List[SourceConfig], suffix: str = ""):
    """Plot a representative 500 ms window of the timeline showing
    top-half preemptions and bottom-half execution."""
    if not state.timeline:
        return

    # Pick a window starting 1 second into the run
    window_start = state.run_start_ts + 1.0
    window_end = window_start + 0.5  # 500 ms

    events = [(ts, ev, src, detail) for (ts, ev, src, detail) in state.timeline
              if window_start <= ts <= window_end]
    if len(events) < 5:
        return

    # Build "lanes" - one per source, plus a top-half lane
    source_names = [s.name for s in sources]
    fig, ax = plt.subplots(figsize=(12, 5), constrained_layout=True)

    # Color per source
    colors = {"HIGH": "#d62728", "MED": "#ff7f0e", "LOW": "#1f77b4"}

    # Plot top-half blocks and bottom-half slices
    th_active = {}  # source -> start_ts
    bh_active = {}  # source -> start_ts

    for ts, ev, src, detail in events:
        t_ms = (ts - window_start) * 1000.0
        if ev == "th_start":
            th_active[src] = ts
        elif ev == "th_end":
            if src in th_active:
                start_ms = (th_active[src] - window_start) * 1000.0
                width_ms = (ts - th_active[src]) * 1000.0
                idx = source_names.index(src) if src in source_names else 0
                ax.barh(idx + 0.5, width_ms, left=start_ms, height=0.3,
                        color=colors.get(src, "gray"), alpha=0.9,
                        edgecolor="black", linewidth=0.5)
                del th_active[src]
        elif ev == "bh_start":
            bh_active[src] = ts
        elif ev == "bh_slice_end":
            if src in bh_active:
                start_ms = (bh_active[src] - window_start) * 1000.0
                width_ms = (ts - bh_active[src]) * 1000.0
                idx = source_names.index(src) if src in source_names else 0
                ax.barh(idx, width_ms, left=start_ms, height=0.3,
                        color=colors.get(src, "gray"), alpha=0.4,
                        edgecolor="black", linewidth=0.5)
                del bh_active[src]

    ax.set_yticks([i for i in range(len(source_names))] +
                  [i + 0.5 for i in range(len(source_names))])
    ax.set_yticklabels([f"{n} BH" for n in source_names] +
                       [f"{n} TH" for n in source_names])
    ax.set_xlabel("Time within window (ms)")
    ax.set_title(f"Timeline: {label}\n(window: 1.0s to 1.5s into run)")
    ax.grid(True, axis="x", alpha=0.3)

    # Legend
    handles = [mpatches.Patch(color=colors.get(s.name, "gray"), alpha=0.9,
                              label=f"{s.name} top half")
               for s in sources]
    handles += [mpatches.Patch(color=colors.get(s.name, "gray"), alpha=0.4,
                               label=f"{s.name} bottom half")
                for s in sources]
    ax.legend(handles=handles, loc="upper right", fontsize=8, ncol=2)

    # Save the chart next to this script file (robust to folder moves)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    out_path = os.path.join(script_dir, f"mp3_3_timeline{suffix}.png")
    plt.savefig(out_path, dpi=120)
    print(f"\nTimeline plot saved to: {out_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    print("=" * 78)
    print("Mini Project 3.3 - Priority-Based Interrupt Latency Simulator")
    print("=" * 78)

    duration = 5.0  # seconds per configuration

    # --- Configuration A: baseline (all short top halves) ---
    sources_a = [
        SourceConfig("HIGH", period_ms=50,  top_half_ms=0.5,
                     bottom_half_ms=5,  priority=1),
        SourceConfig("MED",  period_ms=100, top_half_ms=0.5,
                     bottom_half_ms=10, priority=2),
        SourceConfig("LOW",  period_ms=200, top_half_ms=0.5,
                     bottom_half_ms=15, priority=3),
    ]
    results_a = run_simulation(sources_a, duration,
                               "A. baseline (all short top halves)",
                               record_timeline=True,
                               timeline_suffix="_baseline")

    # --- Configuration B: long top half on HIGH (violates Section 3.5) ---
    # 40 ms top half on a 50 ms period = 80% CPU utilization for HIGH alone,
    # leaving little room for MED/LOW bottom halves. This stresses the
    # system enough to show clear degradation of the lower-priority sources.
    sources_b = [
        SourceConfig("HIGH", period_ms=50,  top_half_ms=40.0,
                     bottom_half_ms=5,  priority=1),
        SourceConfig("MED",  period_ms=100, top_half_ms=0.5,
                     bottom_half_ms=10, priority=2),
        SourceConfig("LOW",  period_ms=200, top_half_ms=0.5,
                     bottom_half_ms=15, priority=3),
    ]
    results_b = run_simulation(sources_b, duration,
                               "B. long top half on HIGH (40 ms / 50 ms period)",
                               record_timeline=True,
                               timeline_suffix="_longtophalf")

    # --- Configuration C: arrival-rate sweep ---
    print(f"\n{'='*78}")
    print("C. Arrival-rate sweep: HIGH source period varied")
    print(f"{'='*78}")

    sweep_results = {}
    for high_period in [20, 50, 100, 200]:
        sources_c = [
            SourceConfig("HIGH", period_ms=high_period, top_half_ms=0.5,
                         bottom_half_ms=5,  priority=1),
            SourceConfig("MED",  period_ms=100, top_half_ms=0.5,
                         bottom_half_ms=10, priority=2),
            SourceConfig("LOW",  period_ms=200, top_half_ms=0.5,
                         bottom_half_ms=15, priority=3),
        ]
        label = f"C. HIGH period = {high_period} ms"
        results = run_simulation(sources_c, duration, label)
        sweep_results[high_period] = results

    # --- Sweep summary ---
    print(f"\n{'='*78}")
    print("SWEEP SUMMARY: LOW source worst-case response latency vs HIGH period")
    print(f"{'='*78}")
    print(f"{'HIGH period (ms)':>16} | {'LOW RL max (ms)':>15} | "
          f"{'LOW RL p95 (ms)':>15} | {'LOW RL mean (ms)':>16}")
    print("-" * 70)
    for period in sorted(sweep_results.keys()):
        low = sweep_results[period].get("LOW", {})
        print(f"{period:>16} | {low.get('rl_max', 0):15.3f} | "
              f"{low.get('rl_p95', 0):15.3f} | "
              f"{low.get('rl_mean', 0):16.3f}")

    # --- Comparison A vs B ---
    print(f"\n{'='*78}")
    print("A vs B comparison: effect of long top half on other sources")
    print(f"{'='*78}")
    print(f"{'Source':>8} | {'Config':>8} | {'RL mean':>8} | {'RL max':>8} | "
          f"{'RL p95':>8}")
    print("-" * 55)
    for src in ["HIGH", "MED", "LOW"]:
        ra = results_a.get(src, {})
        rb = results_b.get(src, {})
        print(f"{src:>8} | {'A':>8} | {ra.get('rl_mean', 0):8.3f} | "
              f"{ra.get('rl_max', 0):8.3f} | {ra.get('rl_p95', 0):8.3f}")
        print(f"{src:>8} | {'B':>8} | {rb.get('rl_mean', 0):8.3f} | "
              f"{rb.get('rl_max', 0):8.3f} | {rb.get('rl_p95', 0):8.3f}")


if __name__ == "__main__":
    main()

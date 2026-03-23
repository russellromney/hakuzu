#!/usr/bin/env python3
"""RSS profiling for hakuzu.

Launches the rss_bench binary and samples RSS (resident set size) at each
reported stage marker. Catches regressions like walrust's 70MB->20MB fix.

Usage:
    python3 bench/measure_rss.py [path-to-rss_bench-binary]

If no path given, defaults to target/release/rss-bench.
"""

import subprocess
import sys
import time
import re
import os

DEFAULT_BINARY = "target/release/rss-bench"


def get_rss_kb(pid: int) -> int:
    """Get RSS in KB for a process on macOS/Linux."""
    try:
        result = subprocess.run(
            ["ps", "-o", "rss=", "-p", str(pid)],
            capture_output=True,
            text=True,
        )
        return int(result.stdout.strip())
    except (ValueError, subprocess.SubprocessError):
        return 0


def main():
    binary = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_BINARY

    if not os.path.exists(binary):
        print(f"Binary not found: {binary}")
        print("Build with: cargo build --release --bin rss-bench")
        sys.exit(1)

    print(f"Launching: {binary}")
    proc = subprocess.Popen(
        [binary],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    stages: list[tuple[str, int]] = []
    stage_pattern = re.compile(r"^STAGE: (.+)$")

    try:
        for line in proc.stdout:
            line = line.strip()
            print(f"  {line}")

            match = stage_pattern.match(line)
            if match:
                stage_name = match.group(1)
                # Small delay to let memory settle after stage transition.
                time.sleep(0.1)
                rss_kb = get_rss_kb(proc.pid)
                stages.append((stage_name, rss_kb))
                print(f"  >> RSS: {rss_kb / 1024:.1f} MB")

        proc.wait()
    except KeyboardInterrupt:
        proc.kill()

    print("\n=== RSS Profile ===")
    print(f"{'Stage':<40} {'RSS (MB)':>10}")
    print("-" * 52)
    for stage_name, rss_kb in stages:
        print(f"{stage_name:<40} {rss_kb / 1024:>10.1f}")

    if len(stages) >= 2:
        peak = max(rss_kb for _, rss_kb in stages)
        final = stages[-1][1]
        print(f"\nPeak: {peak / 1024:.1f} MB | Final: {final / 1024:.1f} MB")


if __name__ == "__main__":
    main()

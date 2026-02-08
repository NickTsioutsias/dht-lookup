"""
DHT Lookup - Run All

One-click script that runs the full demo, benchmark, and generates plots.

Usage:
    python run_all.py          # Full run (demo + full benchmark + plots)
    python run_all.py --quick  # Quick run (demo + quick benchmark + plots)
"""

import argparse
import subprocess
import sys
import os
import time


def run_step(description, command):
    """Run a command and print its output."""
    print()
    print("=" * 60)
    print(f"  {description}")
    print("=" * 60)
    print(f"  Command: {' '.join(command)}")
    print()

    result = subprocess.run(command, cwd=os.path.dirname(os.path.abspath(__file__)))

    if result.returncode != 0:
        print(f"\n  [FAILED] {description} (exit code {result.returncode})")
        return False

    print(f"\n  [OK] {description}")
    return True


def main():
    parser = argparse.ArgumentParser(description="Run the full DHT demo, benchmark, and visualization pipeline.")
    parser.add_argument("--quick", action="store_true", help="Run quick benchmark with smaller parameters")
    parser.add_argument("--skip-demo", action="store_true", help="Skip the demo step")
    parser.add_argument("--skip-benchmark", action="store_true", help="Skip the benchmark step (use existing results)")
    args = parser.parse_args()

    python = sys.executable
    start = time.time()

    print()
    print("########################################################")
    print("#       DHT Lookup - Chord & Pastry Evaluation          #")
    print("########################################################")
    mode = "QUICK" if args.quick else "FULL"
    print(f"  Mode: {mode}")
    print()

    # Step 1: Demo
    if not args.skip_demo:
        ok = run_step("Step 1/3: Demo (all DHT operations)", [python, "main.py"])
        if not ok:
            return 1

    # Step 2: Benchmark
    if not args.skip_benchmark:
        bench_cmd = [python, "-m", "evaluation.benchmark"]
        if args.quick:
            bench_cmd.append("--quick")
        ok = run_step("Step 2/3: Benchmark (experimental evaluation)", bench_cmd)
        if not ok:
            return 1

    # Step 3: Visualization
    ok = run_step("Step 3/3: Generate plots", [python, "-m", "evaluation.visualize"])
    if not ok:
        return 1

    elapsed = time.time() - start

    print()
    print("=" * 60)
    print("  ALL DONE")
    print("=" * 60)
    print(f"  Total time: {elapsed:.1f} seconds")
    print()
    print("  Output files:")
    print("    results/benchmark_results.csv   - Raw benchmark data")
    print("    results/scalability_plot.png     - Hops vs network size")
    print("    results/comparison_bar_chart.png - Chord vs Pastry bar chart")
    print("    results/crud_comparison.png      - CRUD ops with error bars")
    print("    results/summary_table.png        - Summary table")
    print()

    return 0


if __name__ == "__main__":
    sys.exit(main())

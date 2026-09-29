#!/usr/bin/env python3
"""Repeated sequential/concurrent runs; successful throughput and uncertainty."""
import argparse
import json
from pathlib import Path
import statistics
import subprocess
import platform
import datetime
ROOT = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser()
parser.add_argument("--runs", type=int, default=5)
parser.add_argument("--duration", type=int, default=10)
parser.add_argument("--threads", type=int, default=8)
parser.add_argument("--keyspace", type=int, default=1000)
parser.add_argument("--nodes", default="localhost:50051,localhost:50052,localhost:50053,localhost:50054")
parser.add_argument("--output", type=Path, default=ROOT/"benchmark-results"/"latest.json")
parser.add_argument("--durability-label", default="unspecified")
args = parser.parse_args()
if args.runs < 2: parser.error("at least two runs required")
results = {"recorded_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
           "durability": args.durability_label,
           "host": platform.platform(), "notes": "closed-loop localhost; 95% normal-approximation CI; successful latency only",
           "runs": [], "summary": {}}
# Alternate order to reduce systematic warm-cache bias.
for iteration in range(args.runs):
    for mode in (["sequential", "concurrent"] if iteration % 2 == 0 else ["concurrent", "sequential"]):
        command = [str(ROOT/"build/benchmark"), str(args.threads), str(args.duration), str(args.keyspace), "0.5",
                   "--mode", mode, "--nodes", args.nodes]
        process = subprocess.run(command, capture_output=True, text=True)
        if process.returncode not in (0, 2): raise RuntimeError(process.stderr)
        result = json.loads(process.stdout); result["run"] = iteration
        results["runs"].append(result); print(json.dumps(result), flush=True)
for mode in ("sequential", "concurrent"):
    values = [r["successful_ops_per_sec"] for r in results["runs"] if r["mode"] == mode]
    mean, std = statistics.mean(values), statistics.stdev(values)
    results["summary"][mode] = {"mean_successful_ops_per_sec": mean, "stddev": std,
        "normal_95_ci_half_width": 1.96 * std / len(values)**.5}
args.output.parent.mkdir(parents=True, exist_ok=True)
args.output.write_text(json.dumps(results, indent=2))
print(json.dumps(results["summary"], indent=2))

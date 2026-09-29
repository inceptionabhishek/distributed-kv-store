#!/usr/bin/env python3
"""Isolated end-to-end demo: durable writes, failure, recovery, tombstones."""
from pathlib import Path
import subprocess
import tempfile
import time
import argparse
ROOT = Path(__file__).resolve().parents[1]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--benchmark-duration", type=int, default=0)
    parser.add_argument("--benchmark-runs", type=int, default=3)
    parser.add_argument("--benchmark-output", default=str(ROOT/"benchmark-results"/"demo.json"))
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="kv-demo-") as state:
        manager = ["python3", str(ROOT/"scripts/cluster.py"), "--state", state, "--base-port", "52050"]
        ctl = [str(ROOT/"build/kvctl"), "--address", "127.0.0.1:52050"]
        def run(*args):
            result = subprocess.run(args, capture_output=True, text=True)
            if result.returncode:
                raise RuntimeError(f"{args}: {result.stderr}")
            return result.stdout
        try:
            print(run(*manager, "up"))
            print("1. Durable quorum write")
            print(run(*ctl, "put", "demo:key", "before-failure"))
            print(run(*ctl, "replicas", "demo:key"))
            print("2. Kill a replica with SIGKILL; continue writing")
            replicas = run(*ctl, "replicas", "demo:key").splitlines()[1:]
            node = "node" + str(int(replicas[0].rsplit(":", 1)[1]) - 52050)
            print(run(*manager, "kill", node))
            print(run(*ctl, "put", "demo:key", "during-failure"))
            print(run(*ctl, "get", "demo:key"))
            print("3. Restart coordinator; durable hints survive")
            print(run(*manager, "restart", "router"))
            print("4. Recover storage node; wait for automatic convergence")
            print(run(*manager, "restart", node))
            deadline = time.monotonic() + 10
            address = replicas[0]
            while time.monotonic() < deadline:
                result = subprocess.run([str(ROOT/"build/kvctl"), "--address", address, "get", "demo:key"], capture_output=True, text=True)
                if result.returncode == 0 and result.stdout.startswith("during-failure\n"): break
                time.sleep(.1)
            else: raise RuntimeError("replica did not converge")
            print(result.stdout)
            print("5. Delete with tombstone; run anti-entropy")
            print(run(*ctl, "delete", "demo:key"))
            print(run(*ctl, "repair"))
            missing = subprocess.run([*ctl, "get", "demo:key"], capture_output=True, text=True)
            if missing.returncode != 3: raise RuntimeError("deleted key resurfaced")
            print(missing.stdout)
            print("PASS: writes, failure, router restart, replica recovery and deletion")
            if args.benchmark_duration:
                print("6. Repeated benchmark: durable storage, sequential versus concurrent", flush=True)
                subprocess.run(["python3", str(ROOT/"scripts/benchmarks.py"), "--runs", str(args.benchmark_runs),
                    "--duration", str(args.benchmark_duration), "--nodes",
                    ",".join(f"127.0.0.1:{52050+i}" for i in range(1, 5)),
                    "--output", args.benchmark_output, "--durability-label", "always"], check=True)
        finally:
            subprocess.run([*manager, "down"], check=True)
if __name__ == "__main__":
    main()

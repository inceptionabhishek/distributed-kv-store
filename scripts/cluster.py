#!/usr/bin/env python3
"""Local cluster process manager. Uses only the Python standard library."""
import argparse
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import time

ROOT = Path(__file__).resolve().parents[1]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["up", "down", "kill", "restart", "status"])
    parser.add_argument("node", nargs="?", choices=["node1", "node2", "node3", "node4", "router"])
    parser.add_argument("--state", type=Path, default=ROOT / ".data" / "local")
    parser.add_argument("--base-port", type=int, default=50050)
    parser.add_argument("--durability", choices=["always", "periodic", "memory"], default="always")
    parser.add_argument("--delay-ms", type=int, default=0)
    parser.add_argument("--fail-every", type=int, default=0)
    args = parser.parse_args()
    state = args.state.resolve()
    state.mkdir(parents=True, exist_ok=True)
    # Serialize simultaneous manager invocations without touching another directory.
    import fcntl
    with (state / "manager.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        path = state / "processes.json"
        records = json.loads(path.read_text()) if path.exists() else {}
        specs = {}
        nodes = ",".join(f"127.0.0.1:{args.base_port+i}" for i in range(1, 5))
        for i in range(1, 5):
            name = f"node{i}"
            specs[name] = [str(ROOT / "build" / "kv_server"), "--port", str(args.base_port+i),
                           "--data", str(state/name), "--durability", args.durability,
                           "--delay-ms", str(args.delay_ms), "--fail-every", str(args.fail_every)]
        specs["router"] = [str(ROOT / "build" / "kv_router"), "--serve", str(args.base_port),
                           "--data", str(state/"router"), "--nodes", nodes, "--metrics-port", "0"]
        def alive(record):
            result = subprocess.run(["ps", "-p", str(record["pid"]), "-o", "args="], capture_output=True, text=True)
            # Guard against PID reuse before signaling. These exact data directories are task-owned.
            return result.returncode == 0 and record["argv"][0] in result.stdout and str(state/record["name"]) in result.stdout
        def stop(name, hard=False):
            record = records.get(name)
            if record and alive(record):
                os.kill(record["pid"], signal.SIGKILL if hard else signal.SIGTERM)
                for _ in range(100):
                    if not alive(record): break
                    time.sleep(.05)
                if alive(record): raise RuntimeError(f"{name} did not stop; refusing to start another owner")
            records.pop(name, None)
        def start(name):
            if name in records and alive(records[name]): return
            argv = specs[name]
            port = args.base_port if name == "router" else args.base_port + int(name[-1])
            with socket.socket() as probe:
                probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                probe.bind(("127.0.0.1", port))  # fail before launching if unrelated listener owns port
            with (state/f"{name}.log").open("ab") as output:
                process = subprocess.Popen(argv, stdout=output, stderr=output, start_new_session=True)
            records[name] = {"name": name, "pid": process.pid, "argv": argv}
            for _ in range(100):
                if process.poll() is not None: raise RuntimeError(f"{name} exited; see {state/name}.log")
                with socket.socket() as probe:
                    probe.settimeout(.1)
                    if probe.connect_ex(("127.0.0.1", port)) == 0: return
                time.sleep(.05)
            raise RuntimeError(f"{name} startup timed out")
        try:
            if args.command == "up":
                for name in specs: start(name)
            elif args.command == "down":
                for name in reversed(specs): stop(name)
            elif args.command in ("kill", "restart"):
                if not args.node: parser.error("node is required")
                stop(args.node, hard=args.command == "kill")
                if args.command == "restart": start(args.node)
            for name in specs:
                print(f"{name}: {'UP' if name in records and alive(records[name]) else 'DOWN'}")
        finally:
            temp = state / "processes.json.tmp"
            temp.write_text(json.dumps(records, indent=2))
            temp.replace(path)

if __name__ == "__main__":
    main()

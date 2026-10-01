"""One-shot collector supervisor with a deadline and per-run UTF-8 evidence."""

from __future__ import annotations

import argparse
import json
import os
import ssl
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from news_pipeline.rss import atomic_write_json


def supervise(command: list[str], log_root: Path, timeout: float) -> dict:
    if timeout <= 0:
        raise ValueError("timeout must be positive")
    log_root.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid.uuid4().hex
    evidence = log_root / f"runner_{run_id}.json"
    stdout_path = log_root / f"runner_{run_id}.stdout.log"
    stderr_path = log_root / f"runner_{run_id}.stderr.log"
    result = {
        "status": "started", "runner_id": run_id,
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "runner_pid": os.getpid(), "python": sys.executable, "cwd": str(ROOT),
        "command": command, "timeout_seconds": timeout,
        "stdout_path": str(stdout_path.resolve()), "stderr_path": str(stderr_path.resolve()),
        "evidence_path": str(evidence.resolve()),
        "tls_verify_paths": ssl.get_default_verify_paths()._asdict(),
        "tls_cert_store": ssl.create_default_context().cert_store_stats(),
    }
    atomic_write_json(evidence, result)
    started = time.monotonic()
    try:
        env = {**os.environ, "PYTHONIOENCODING": "utf-8", "PYTHONUNBUFFERED": "1"}
        with stdout_path.open("xb") as stdout, stderr_path.open("xb") as stderr:
            child = subprocess.Popen(
                command, cwd=ROOT, env=env, stdout=stdout, stderr=stderr,
                creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
            )
            result["child_pid"] = child.pid
            atomic_write_json(evidence, result)
            try:
                result["exit_code"] = child.wait(timeout=timeout)
                result["status"] = "completed" if result["exit_code"] == 0 else "failed"
            except subprocess.TimeoutExpired:
                # Collector spawns no subprocesses; kill only this owned child.
                child.kill()
                child.wait()
                result.update(status="timed_out", exit_code=124, child_exit_code=child.returncode)
    except Exception as exc:
        result.update(status="failed", exit_code=1, error_type=type(exc).__name__, error=str(exc))
    finally:
        result["ended_at_utc"] = datetime.now(timezone.utc).isoformat()
        result["duration_seconds"] = round(time.monotonic() - started, 3)
        atomic_write_json(evidence, result)
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--timeout-seconds", type=float, default=540)
    parser.add_argument("--log-root", type=Path, default=ROOT / "runtime/data_collection_v2/logs/runs")
    args = parser.parse_args()
    if not 1 <= args.timeout_seconds <= 540:
        parser.error("timeout must be 1..540 seconds, below the 10-minute schedule")
    command = [
        sys.executable, str(ROOT / "scripts/collect_rss.py"),
        "--config", str(ROOT / "config/rss_sources.yaml"),
        "--raw-root", str(ROOT / "runtime/data_collection_v2/raw/rss"),
        "--state", str(ROOT / "runtime/data_collection_v2/state.json"),
        "--lock", str(ROOT / "runtime/data_collection_v2/collector.lock"),
        "--evidence", str(ROOT / "evidence/d02/collect_manifest.json"), "--request-gap", "5",
    ]
    result = supervise(command, args.log_root, args.timeout_seconds)
    print(json.dumps(result, ensure_ascii=True, sort_keys=True))
    return result["exit_code"]


if __name__ == "__main__":
    raise SystemExit(main())

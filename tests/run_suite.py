#!/usr/bin/env python3
"""Run the test suite one file at a time, with a per-file timeout.

Why this exists: on a slow or constrained machine a single stalled test (or a
process that will not exit) makes ``pytest -q`` look like a hang.  This runner
bounds every file, prints a PASS/FAIL line per file, and tells you exactly which
one misbehaved instead of leaving you with silence.

    python tests/run_suite.py                 # everything, 180s per file
    python tests/run_suite.py --fast          # skip tests marked slow
    python tests/run_suite.py --timeout 60 --pytest-args "-x"

Exit code is 0 only when every file passed.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--timeout", type=float, default=180.0, help="seconds per test file")
    parser.add_argument("--fast", action="store_true", help="skip tests marked slow")
    parser.add_argument("--pytest-args", default="", help="extra arguments for pytest")
    args = parser.parse_args(argv)

    root = Path(__file__).resolve().parent
    files = sorted(root.glob("test_*.py"))
    if not files:
        print("no test files found", file=sys.stderr)
        return 1

    extra = args.pytest_args.split() if args.pytest_args else []
    if args.fast:
        extra += ["-m", "not slow"]

    failures: list[str] = []
    started = time.monotonic()

    for path in files:
        command = [sys.executable, "-m", "pytest", "-q", str(path), *extra]
        begin = time.monotonic()
        try:
            result = subprocess.run(command, capture_output=True, text=True, timeout=args.timeout)
        except subprocess.TimeoutExpired:
            elapsed = time.monotonic() - begin
            print(f"TIMEOUT  {path.name:<28} > {args.timeout:.0f}s ({elapsed:.0f}s)")
            failures.append(path.name)
            continue
        except FileNotFoundError:  # pragma: no cover
            print("pytest is not installed. Install it with:", file=sys.stderr)
            print('    pip install "pytest>=8" "pytest-asyncio>=0.23"', file=sys.stderr)
            return 2

        elapsed = time.monotonic() - begin
        output = (result.stdout or "") + (result.stderr or "")
        if "No module named pytest" in output:
            print("pytest is not installed. Install it with:", file=sys.stderr)
            print('    pip install "pytest>=8" "pytest-asyncio>=0.23"', file=sys.stderr)
            return 2

        summary = ""
        for line in reversed(output.strip().splitlines()):
            if " passed" in line or " failed" in line or "error" in line.lower():
                summary = line.strip()
                break

        if result.returncode == 0:
            print(f"PASS     {path.name:<28} {elapsed:5.1f}s  {summary}")
        elif result.returncode == 5:  # pytest: nothing selected (e.g. --fast)
            print(f"SKIP     {path.name:<28} {elapsed:5.1f}s  all tests deselected")
        else:
            print(f"FAIL     {path.name:<28} {elapsed:5.1f}s  {summary}")
            failures.append(path.name)
            tail = [line for line in output.strip().splitlines() if line.strip()][-15:]
            for line in tail:
                print(f"         {line}")

    total = time.monotonic() - started
    print("-" * 72)
    if failures:
        print(f"{len(failures)} file(s) failed or timed out: {', '.join(failures)}")
        return 1
    print(f"all {len(files)} test files passed in {total:.1f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""
Main caller that runs all data/scripts fetchers concurrently via asyncio subprocesses.

For each fetcher:
  - `--end`   defaults to today (Pacific); override with --end YYYYMMDD.
  - `--start` is derived per-fetcher from the max datetime_pt in the fetcher's CSV
    minus --overlap-days (default 7). If the CSV is missing/empty, start falls back
    to end - --default-start-days (default 365).
  - `--tag`   is forwarded to every fetcher when provided.

On subprocess failure, writes data/logs/{YYYYMMDD_HHMMSS}_{script_stem}.log with the
full invocation, returncode, stdout, and stderr. Successful runs produce no log file.

Usage:
    python fetch_all.py [--end YYYYMMDD] [--overlap-days N] [--tag TAG] [--default-start-days N]
"""

import argparse
import asyncio
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "..")
LOGS_DIR = os.path.join(DATA_DIR, "logs")

# (script filename, CSV stem). Datetime column is `datetime_pt` for all three.
FETCHERS = [
    ("fetch_caiso_fuelmix.py", "caiso_fuelmix"),
    ("fetch_eia_demand.py", "eia_demand"),
    ("fetch_sgip_mer.py", "sgip_mer"),
]
DATETIME_COL = "datetime_pt"


@dataclass
class FetcherResult:
    script: str
    cmd: list[str]
    start: str
    end: str
    returncode: int
    stdout: str
    stderr: str
    duration: float
    log_path: str | None = None

    @property
    def ok(self) -> bool:
        return self.returncode == 0


def csv_path_for(stem: str, tag: str | None) -> str:
    filename = f"{stem}_{tag}.csv" if tag else f"{stem}.csv"
    return os.path.join(DATA_DIR, filename)


def compute_start(csv_path: str, end_date: datetime, overlap_days: int, default_start_days: int) -> datetime:
    """Return start date for a fetcher based on its existing CSV, or a fallback."""
    if os.path.exists(csv_path) and os.path.getsize(csv_path) > 0:
        try:
            df = pd.read_csv(csv_path, usecols=[DATETIME_COL])
            if not df.empty:
                last = pd.to_datetime(df[DATETIME_COL], utc=True).max()
                last_date = last.tz_convert("America/Los_Angeles").to_pydatetime().replace(tzinfo=None)
                start = last_date - timedelta(days=overlap_days)
                # Normalize to midnight and clamp to end_date.
                start = datetime(start.year, start.month, start.day)
                return min(start, end_date)
        except (ValueError, KeyError) as e:
            print(f"  [warn] could not read {csv_path}: {e}. Falling back to default start.")
    return end_date - timedelta(days=default_start_days)


async def run_fetcher(
    script: str, start: str, end: str, tag: str | None, run_ts: str
) -> FetcherResult:
    """Launch one fetcher subprocess and capture its output."""
    script_path = os.path.join(SCRIPT_DIR, script)
    tag_args = ["--tag", tag] if tag else []
    cmd = [sys.executable, script_path, "--start", start, "--end", end, *tag_args]

    print(f"[{script}] starting: --start {start} --end {end}" + (f" --tag {tag}" if tag else ""))
    t0 = time.monotonic()
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout_b, stderr_b = await proc.communicate()
    duration = time.monotonic() - t0
    stdout = stdout_b.decode("utf-8", errors="replace")
    stderr = stderr_b.decode("utf-8", errors="replace")
    rc = proc.returncode if proc.returncode is not None else -1

    result = FetcherResult(
        script=script, cmd=cmd, start=start, end=end,
        returncode=rc, stdout=stdout, stderr=stderr, duration=duration,
    )

    if not result.ok:
        stem = os.path.splitext(script)[0]
        log_path = os.path.join(LOGS_DIR, f"{run_ts}_{stem}.log")
        with open(log_path, "w", encoding="utf-8") as f:
            f.write(f"command: {' '.join(cmd)}\n")
            f.write(f"returncode: {rc}\n")
            f.write(f"duration_seconds: {duration:.2f}\n")
            f.write("\n===== stdout =====\n")
            f.write(stdout)
            f.write("\n===== stderr =====\n")
            f.write(stderr)
        result.log_path = log_path
        print(f"[{script}] FAILED (rc={rc}) in {duration:.1f}s -> {log_path}")
    else:
        print(f"[{script}] OK in {duration:.1f}s")

    return result


async def main_async(args: argparse.Namespace) -> int:
    pt = ZoneInfo("America/Los_Angeles")
    if args.end:
        end_date = datetime.strptime(args.end, "%Y%m%d")
    else:
        end_date = datetime.now(pt).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
    end_str = end_date.strftime("%Y%m%d")

    os.makedirs(LOGS_DIR, exist_ok=True)
    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    coroutines = []
    planned = []
    for script, stem in FETCHERS:
        csv_path = csv_path_for(stem, args.tag)
        start_date = compute_start(csv_path, end_date, args.overlap_days, args.default_start_days)
        start_str = start_date.strftime("%Y%m%d")
        planned.append((script, start_str, end_str, csv_path))
        coroutines.append(run_fetcher(script, start_str, end_str, args.tag, run_ts))

    print("Plan:")
    for script, s, e, csv in planned:
        print(f"  {script}: {s} -> {e}  (from {csv})")
    print()

    results: list[FetcherResult] = await asyncio.gather(*coroutines)

    # Summary
    print("\n===== Summary =====")
    print(f"{'script':32} {'start':>10} {'end':>10} {'status':>6} {'dur(s)':>8}  log")
    for r in results:
        status = "OK" if r.ok else "FAIL"
        log = r.log_path or ""
        print(f"{r.script:32} {r.start:>10} {r.end:>10} {status:>6} {r.duration:>8.1f}  {log}")

    return 0 if all(r.ok for r in results) else 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Run all CAISO portfolio fetchers concurrently.")
    parser.add_argument("--end", default=None, help="End date YYYYMMDD (exclusive). Default: today PT.")
    parser.add_argument("--overlap-days", type=int, default=7,
                        help="Days of overlap subtracted from each CSV's max datetime. Default: 7.")
    parser.add_argument("--tag", default=None, help="Optional tag forwarded to every fetcher.")
    parser.add_argument("--default-start-days", type=int, default=365,
                        help="If a CSV is missing/empty, start = end - this many days. Default: 365.")
    args = parser.parse_args()
    return asyncio.run(main_async(args))


if __name__ == "__main__":
    sys.exit(main())

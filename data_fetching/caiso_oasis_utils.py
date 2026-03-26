"""
caiso_oasis_utils.py
--------------------
Shared utilities for all CAISO OASIS data pulls.
No dataset-specific logic lives here — only the HTTP layer,
date chunking, and file I/O.

Spec reference:
  https://www.caiso.com/Documents/OASIS-InterfaceSpecification_v4_3_5Clean_Spring2017Release.pdf
  Version 4.3.5, Spring 2017
"""

import io
import time
import zipfile
import urllib.parse
from datetime import date, timedelta
from pathlib import Path
from typing import Iterator, Optional

import pandas as pd
import requests

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

OASIS_ENDPOINT = "https://oasis.caiso.com/oasisapi/SingleZip"
API_VERSION    = "1"
CSV_FORMAT     = "6"          # resultformat=6 returns CSV inside the ZIP

# CAISO enforces a hard rate limit of 1 request per 5 seconds.
# Requests that exceed this receive a 429 status code.
# Reference: CAISO OASIS Release Notes v1.1 (Fall 2015)
# gridstatus (the reference implementation) uses 5s as its default.
# We use 6s to stay comfortably under the limit.
REQUEST_PAUSE_SECONDS = 6

# On a 429, wait this long before retrying (seconds).
RETRY_PAUSE_SECONDS = 30
MAX_RETRIES = 3

# Project date range
PROJECT_START = date(2025, 1, 1)
PROJECT_END   = date(2026, 3, 22)

# Output directory — all saved files land here
OUTPUT_DIR = Path("caiso_data")
OUTPUT_DIR.mkdir(exist_ok=True)


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------

def oasis_datetime(d: date, end_of_day: bool = False) -> str:
    """
    Format a date as an OASIS API datetime string in GMT.
    Start of day: YYYYMMDDThh:mm-0000 → T00:00-0000
    End of day:   YYYYMMDDThh:mm-0000 → T23:59-0000
    """
    if end_of_day:
        return d.strftime("%Y%m%dT23:59-0000")
    return d.strftime("%Y%m%dT00:00-0000")


def date_chunks(
    start: date,
    end: date,
    chunk_days: int,
) -> Iterator[tuple[date, date]]:
    """
    Yield (chunk_start, chunk_end) pairs spanning [start, end].
    Respects OASIS query size limits — callers choose chunk_days
    appropriate for the dataset density.
    """
    cur = start
    while cur <= end:
        chunk_end = min(cur + timedelta(days=chunk_days - 1), end)
        yield cur, chunk_end
        cur = chunk_end + timedelta(days=1)


# ---------------------------------------------------------------------------
# HTTP layer
# ---------------------------------------------------------------------------

def fetch_oasis_csv(
    params: dict,
    label: str,
) -> Optional[pd.DataFrame]:
    """
    Call the OASIS SingleZip endpoint with the given params,
    unzip the response, parse the first CSV file found inside,
    and return a DataFrame.

    Returns None on any failure — callers decide how to handle gaps.

    Always injects: version, resultformat (CSV).
    Caller must supply: queryname, market_run_id, startdatetime, enddatetime,
    plus any dataset-specific filter parameters.

    Rate limiting:
      CAISO enforces 1 request per 5 seconds (429 on violation).
      This function always sleeps REQUEST_PAUSE_SECONDS after each
      request and retries up to MAX_RETRIES times on a 429.
    """
    params = {
        **params,
        "version":      API_VERSION,
        "resultformat": CSV_FORMAT,
    }
    url = OASIS_ENDPOINT + "?" + urllib.parse.urlencode(params)

    print(f"  [{label}] GET ...", end=" ", flush=True)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, timeout=120)

            # 429 = rate limited — back off and retry
            if resp.status_code == 429:
                print(
                    f"429 rate limited (attempt {attempt}/{MAX_RETRIES}) "
                    f"— waiting {RETRY_PAUSE_SECONDS}s ...",
                    end=" ", flush=True,
                )
                time.sleep(RETRY_PAUSE_SECONDS)
                continue

            resp.raise_for_status()

            z = zipfile.ZipFile(io.BytesIO(resp.content))
            csv_names = [n for n in z.namelist() if n.lower().endswith(".csv")]
            if not csv_names:
                xml_names = [n for n in z.namelist() if n.lower().endswith(".xml")]
                if xml_names:
                    print(f"no CSV in ZIP — found {len(xml_names)} XML file(s):")
                    for xml_name in xml_names:
                        print(f"    [{xml_name}]")
                        print(z.read(xml_name).decode("utf-8", errors="replace"))
                else:
                    print(f"no CSV in ZIP — contents: {z.namelist()}")
                time.sleep(REQUEST_PAUSE_SECONDS)
                return None

            with z.open(csv_names[0]) as f:
                df = pd.read_csv(f, low_memory=False)

            print(f"{len(df):,} rows")
            time.sleep(REQUEST_PAUSE_SECONDS)
            return df

        except requests.exceptions.HTTPError as e:
            print(f"HTTP {e.response.status_code} — skipping")
            break
        except zipfile.BadZipFile:
            print("bad ZIP — skipping")
            break
        except Exception as e:
            print(f"ERROR: {e} — skipping")
            break

    time.sleep(REQUEST_PAUSE_SECONDS)
    return None


# ---------------------------------------------------------------------------
# File I/O
# ---------------------------------------------------------------------------

def save(frames: list[pd.DataFrame], stem: str) -> pd.DataFrame:
    """
    Concatenate a list of DataFrames, drop exact duplicates,
    and save to both parquet and CSV under OUTPUT_DIR.

    Returns the combined DataFrame (empty DataFrame if frames is empty).
    """
    if not frames:
        print(f"  [save] No data for '{stem}' — nothing written.")
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True).drop_duplicates()

    parquet_path = OUTPUT_DIR / f"{stem}.parquet"
    csv_path     = OUTPUT_DIR / f"{stem}.csv"

    df.to_parquet(parquet_path, index=False)
    df.to_csv(csv_path, index=False)

    print(f"  [save] {len(df):,} rows → {parquet_path}")
    return df

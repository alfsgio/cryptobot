import requests
import numpy as np
import sqlite3
import os
import calendar
import hashlib
import time
from datetime import datetime, timezone

COIN = "HYPE"
SYMBOL = "HYPE/USDC:USDC"
EXCHANGE = "hyperliquid"
TF = "1m"

BASE_DIR = "/app/caches/ohlcvs/data/hyperliquid/1m/HYPE_USDC_USDC"
CATALOG_PATH = "/app/caches/ohlcvs/catalog.sqlite"

START_YEAR, START_MONTH = 2024, 11   # HYPE launched Nov 29, 2024
END_DT = datetime.now(timezone.utc)
END_YEAR, END_MONTH = END_DT.year, END_DT.month


def fetch_candles(coin, start_ms, end_ms):
    url = "https://api.hyperliquid.xyz/info"
    payload = {
        "type": "candleSnapshot",
        "req": {"coin": coin, "interval": "1m", "startTime": start_ms, "endTime": end_ms},
    }
    for attempt in range(3):
        try:
            r = requests.post(url, json=payload, timeout=30)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            print(f"  fetch error ({attempt+1}/3): {e}")
            time.sleep(2)
    return []


def month_info(year, month):
    n_days = calendar.monthrange(year, month)[1]
    n_minutes = n_days * 24 * 60
    start_ms = int(datetime(year, month, 1, tzinfo=timezone.utc).timestamp() * 1000)
    end_ms = start_ms + n_minutes * 60000 - 60000  # last minute of month
    return n_minutes, start_ms, end_ms


def download_month(year, month):
    n_minutes, start_ms, end_ms = month_info(year, month)
    data = np.full((n_minutes, 4), np.nan, dtype=np.float32)
    valid = np.zeros(n_minutes, dtype=bool)

    batch_start = start_ms
    total_fetched = 0
    while batch_start <= end_ms:
        batch_end = min(batch_start + 5000 * 60000, end_ms)
        candles = fetch_candles(COIN, batch_start, batch_end)
        if candles:
            for c in candles:
                t = c["t"]
                if t < start_ms or t > end_ms:
                    continue
                idx = (t - start_ms) // 60000
                if 0 <= idx < n_minutes:
                    data[idx] = [float(c["h"]), float(c["l"]), float(c["c"]), float(c["v"])]
                    valid[idx] = True
            total_fetched += len(candles)
        batch_start = batch_end + 60000
        time.sleep(0.15)

    print(f"  {year}/{month:02d}: {valid.sum()}/{n_minutes} candles valid (fetched {total_fetched} rows)")
    return data, valid, start_ms, end_ms


def compute_checksum(data, valid):
    return hashlib.sha256(data.tobytes() + valid.tobytes()).hexdigest()


def main():
    os.makedirs(BASE_DIR, exist_ok=True)

    conn = sqlite3.connect(CATALOG_PATH)
    cur = conn.cursor()

    # Clean up stale HYPE entries
    cur.execute("DELETE FROM gaps WHERE symbol=? AND exchange=?", (SYMBOL, EXCHANGE))
    cur.execute("DELETE FROM chunks WHERE symbol=? AND exchange=?", (SYMBOL, EXCHANGE))
    cur.execute("DELETE FROM symbols WHERE symbol=? AND exchange=?", (SYMBOL, EXCHANGE))
    conn.commit()

    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    first_ts = None
    last_ts = None

    year, month = START_YEAR, START_MONTH
    while (year, month) <= (END_YEAR, END_MONTH):
        print(f"Downloading {year}/{month:02d}...")
        data, valid, start_ms, end_ms = download_month(year, month)

        year_dir = os.path.join(BASE_DIR, str(year))
        os.makedirs(year_dir, exist_ok=True)
        body_path = os.path.join(year_dir, f"{month:02d}.npy")
        valid_path = os.path.join(year_dir, f"{month:02d}.valid.npy")

        np.save(body_path, data)
        np.save(valid_path, valid)

        checksum = compute_checksum(data, valid)
        is_current = (year == END_YEAR and month == END_MONTH)
        status = "open" if is_current else "done"

        cur.execute("""
            INSERT INTO chunks
              (exchange, timeframe, symbol, year, month, body_path, valid_path,
               start_ts, end_ts, rows, status, schema_version, checksum, updated_at)
            VALUES (?, '1m', ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
        """, (EXCHANGE, SYMBOL, year, month, body_path, valid_path,
              start_ms, end_ms, len(data), status, checksum, now_ms))
        conn.commit()

        if first_ts is None:
            first_ts = start_ms
        last_ts = end_ms

        if month == 12:
            year, month = year + 1, 1
        else:
            month += 1

    if first_ts:
        cur.execute("""
            INSERT OR REPLACE INTO symbols (exchange, timeframe, symbol, first_ts, last_ts, updated_at)
            VALUES (?, '1m', ?, ?, ?, ?)
        """, (EXCHANGE, SYMBOL, first_ts, last_ts, now_ms))
        conn.commit()

    conn.close()
    print("\nDone! All HYPE data downloaded.")


if __name__ == "__main__":
    main()

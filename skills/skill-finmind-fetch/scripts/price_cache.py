"""Shared TaiwanStockPrice cache.

Types 1, 5, 8, 11, 12, 17 and 18 each fetch TaiwanStockPrice for the same
stock independently within one daily run, which burns FinMind quota on
identical requests. When a run directory is supplied, the first fetch for a
stock is cached to disk (as the raw API JSON records) and later adapters in
the same run reuse it instead of hitting the API again. Falls back to a live
fetch whenever no cache dir is given, the cache is missing, unreadable, or
does not fully cover the requested date range, so behavior for a standalone
script invocation is unchanged.
"""
from __future__ import annotations
import json
from pathlib import Path

import pandas as pd


def cached_price(stock_id, start_date, end_date, token, cache_dir, fetch_data):
    if not cache_dir:
        return fetch_data("TaiwanStockPrice", data_id=stock_id, start_date=start_date, end_date=end_date, token=token)

    cache_path = Path(cache_dir) / f"{stock_id}.json"
    if cache_path.exists():
        try:
            records = json.loads(cache_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            records = None
        if records:
            dates = [r.get("date") for r in records if r.get("date")]
            if dates and min(dates) <= start_date and max(dates) >= end_date:
                df = pd.DataFrame(records)
                mask = (df["date"] >= start_date) & (df["date"] <= end_date)
                return df[mask].reset_index(drop=True)

    df = fetch_data("TaiwanStockPrice", data_id=stock_id, start_date=start_date, end_date=end_date, token=token)
    if not df.empty:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(df.to_json(orient="records"), encoding="utf-8")
    return df

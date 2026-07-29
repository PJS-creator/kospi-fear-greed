import json
import urllib.parse
from datetime import datetime

import tmp_intraday_analysis as analysis


SYMBOLS = {
    "KOSPI200": "^KS200",
    "005930": "005930.KS",
    "000660": "000660.KS",
}


def no_naver(code: str):
    return {}, {"mode": "disabled_for_full_month_consistency", "unique_dates": 0, "bars": 0, "errors": []}


def fetch_daily(symbol: str):
    p1 = int(datetime(2026, 6, 29, 0, 0, tzinfo=analysis.KST).timestamp())
    p2 = int(datetime(2026, 7, 30, 0, 0, tzinfo=analysis.KST).timestamp())
    params = {
        "period1": p1,
        "period2": p2,
        "interval": "1d",
        "includePrePost": "false",
        "events": "div,splits",
    }
    payload = None
    for host in ("query1.finance.yahoo.com", "query2.finance.yahoo.com"):
        url = f"https://{host}/v8/finance/chart/{urllib.parse.quote(symbol, safe='')}?{urllib.parse.urlencode(params)}"
        try:
            candidate = analysis.get_json(url, retries=4)
            if ((candidate.get("chart") or {}).get("result") or []):
                payload = candidate
                break
        except Exception:
            pass
    if not payload:
        return {}
    result = payload["chart"]["result"][0]
    timestamps = result.get("timestamp") or []
    quote = ((result.get("indicators") or {}).get("quote") or [{}])[0]
    out = {}
    for i, epoch in enumerate(timestamps):
        try:
            ds = datetime.fromtimestamp(int(epoch), tz=analysis.KST).strftime("%Y%m%d")
            row = {
                "open": float((quote.get("open") or [])[i]),
                "high": float((quote.get("high") or [])[i]),
                "low": float((quote.get("low") or [])[i]),
                "close": float((quote.get("close") or [])[i]),
            }
        except (IndexError, TypeError, ValueError):
            continue
        out[ds] = row
    return out


DAILY = {key: fetch_daily(symbol) for key, symbol in SYMBOLS.items()}
ORIGINAL_BUILD_DAILY = analysis.build_daily


def build_daily_with_official_ohlc(bar_days, asset_key, source):
    rows = ORIGINAL_BUILD_DAILY(bar_days, asset_key, source)
    official = DAILY.get(asset_key, {})
    previous_close = None
    for row in rows:
        day = official.get(row["date"])
        if day:
            row["intraday_last_close"] = row["close"]
            row["intraday_high"] = row["high"]
            row["intraday_low"] = row["low"]
            row["open"] = day["open"]
            row["high"] = day["high"]
            row["low"] = day["low"]
            row["close"] = day["close"]
        row["previous_close"] = previous_close
        row["return_prev_pct"] = ((row["close"] / previous_close - 1) * 100) if previous_close else None
        row["return_open_pct"] = (row["close"] / row["open"] - 1) * 100
        row["open_gap_pct"] = ((row["open"] / previous_close - 1) * 100) if previous_close else None
        row["open_is_exact_high"] = abs(row["open"] - row["high"]) <= max(1e-9, abs(row["high"]) * 1e-10)
        row["open_within_0_3pct_of_high"] = (row["high"] / row["open"] - 1) <= 0.0030000001
        for key in ("open", "10:00-10:30", "12:00-12:30", "14:00-14:30", "close"):
            row[key + "_index"] = row[key] / row["open"] * 100 if row.get(key) is not None else None
        previous_close = row["close"]
    return rows


analysis.fetch_naver_intraday = no_naver
analysis.build_daily = build_daily_with_official_ohlc
analysis.main()

with open("tmp_intraday_results.json", "r", encoding="utf-8") as f:
    result = json.load(f)
result["definitions"]["official_ohlc"] = "Yahoo daily OHLC overrides intraday first/last/high/low; 5-minute bars are used only for the three 30-minute windows and high/low timing buckets."
for key in result["assets"]:
    result["assets"][key]["source"] = "Yahoo 5m intraday + Yahoo 1d official OHLC"
    for row in result["assets"][key]["daily"]:
        row["source"] = "Yahoo 5m intraday + Yahoo 1d official OHLC"
with open("tmp_intraday_results.json", "w", encoding="utf-8") as f:
    json.dump(result, f, ensure_ascii=False, indent=2)

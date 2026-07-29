# 2026-07 Korean intraday analysis diagnostics

Generated: 2026-07-30T03:50:46.242546+09:00

## 코스피200 (KOSPI200)
- source: Yahoo ^KS200 5m
- trading days: 20 / complete: 20
- down days: 12
- all-day open100 mean: {'open': 100.0, '10:00-10:30': 99.61363674527975, '12:00-12:30': 98.06071589664427, '14:00-14:30': 98.20175685894093, 'close': 98.29762098656207}
- down-day open100 mean: {'open': 100.0, '10:00-10:30': 99.1104232804902, '12:00-12:30': 96.54611189265393, '14:00-14:30': 96.53714670031557, 'close': 96.60390168962884}
- down-day near-open-high: 1/12
- down-day low buckets: {'10:31~12:30': 4, '09:00~10:30': 1, '12:31~14:30': 6, '14:31~종가': 1}

## 삼성전자 (005930)
- source: Naver 10m
- trading days: 6 / complete: 6
- down days: 3
- all-day open100 mean: {'open': 100.0, '10:00-10:30': 98.20557159412404, '12:00-12:30': 95.35762210189718, '14:00-14:30': 95.34444640585171, 'close': 95.28027486737928}
- down-day open100 mean: {'open': 100.0, '10:00-10:30': 97.68628437457528, '12:00-12:30': 92.45428943639767, '14:00-14:30': 92.4697632085989, 'close': 92.6977197321542}
- down-day near-open-high: 1/3
- down-day low buckets: {'10:31~12:30': 1, '14:31~종가': 1, '12:31~14:30': 1}

## SK하이닉스 (000660)
- source: Naver 10m
- trading days: 6 / complete: 6
- down days: 3
- all-day open100 mean: {'open': 100.0, '10:00-10:30': 97.94466174677676, '12:00-12:30': 95.2240746288554, '14:00-14:30': 95.11824341693352, 'close': 94.78096965584405}
- down-day open100 mean: {'open': 100.0, '10:00-10:30': 96.79745954167363, '12:00-12:30': 92.32810457010267, '14:00-14:30': 92.03960238381546, 'close': 91.84662283643995}
- down-day near-open-high: 1/3
- down-day low buckets: {'12:31~14:30': 2, '14:31~종가': 1}

## Validations
```json
{
  "005930": {
    "overlap_dates": 6,
    "mean_abs_open_diff_bps": 0.0,
    "max_abs_open_diff_bps": 0.0,
    "mean_abs_close_diff_bps": 65.04522744643951,
    "max_abs_close_diff_bps": 151.2287334593576
  },
  "000660": {
    "overlap_dates": 6,
    "mean_abs_open_diff_bps": 0.0,
    "max_abs_open_diff_bps": 0.0,
    "mean_abs_close_diff_bps": 92.30447020539083,
    "max_abs_close_diff_bps": 166.57710908113964
  },
  "KOSPI200_vs_KODEX200": {
    "overlap_dates": 6,
    "corr_intraday_close_open_return": 0.9583374901776387,
    "mean_abs_return_diff_pct_point": 0.6156048709182609
  }
}
```

## Diagnostics
```json
{
  "naver_005930": {
    "mode": "daily",
    "errors": [],
    "unique_dates": 6,
    "bars": 230
  },
  "naver_000660": {
    "mode": "daily",
    "errors": [],
    "unique_dates": 6,
    "bars": 230
  },
  "naver_069500": {
    "mode": "daily",
    "errors": [],
    "unique_dates": 6,
    "bars": 230
  },
  "yahoo_KOSPI200": {
    "host": "query1.finance.yahoo.com",
    "symbol": "^KS200",
    "exchange_timezone": "Asia/Seoul",
    "unique_dates": 22,
    "bars": 1585
  },
  "yahoo_005930": {
    "host": "query1.finance.yahoo.com",
    "symbol": "005930.KS",
    "exchange_timezone": "Asia/Seoul",
    "unique_dates": 22,
    "bars": 1565
  },
  "yahoo_000660": {
    "host": "query1.finance.yahoo.com",
    "symbol": "000660.KS",
    "exchange_timezone": "Asia/Seoul",
    "unique_dates": 22,
    "bars": 1565
  }
}
```
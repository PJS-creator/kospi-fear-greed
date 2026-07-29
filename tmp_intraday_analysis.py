#!/usr/bin/env python3
from __future__ import annotations

import json
import math
import statistics
import time
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

KST = ZoneInfo('Asia/Seoul')
START = '20260630'
END = '20260729'
JULY_START = '20260701'
JULY_END = '20260729'
STRESS_DATES = {'20260728', '20260729'}
UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/126 Safari/537.36'

ASSETS = {
    'KOSPI200': {'name': '코스피200', 'primary': 'yahoo', 'symbol': '^KS200', 'proxy_code': '069500'},
    '005930': {'name': '삼성전자', 'primary': 'naver', 'symbol': '005930.KS'},
    '000660': {'name': 'SK하이닉스', 'primary': 'naver', 'symbol': '000660.KS'},
}


def get_json(url: str, retries: int = 4, timeout: int = 30):
    last = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={'User-Agent': UA, 'Accept': 'application/json,text/plain,*/*'})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                raw = r.read()
            return json.loads(raw.decode('utf-8'))
        except Exception as e:
            last = e
            time.sleep(1.2 * (attempt + 1))
    raise RuntimeError(f'GET failed after {retries} attempts: {url}: {last}')


def daterange(start: str, end: str):
    d0 = datetime.strptime(start, '%Y%m%d').date()
    d1 = datetime.strptime(end, '%Y%m%d').date()
    d = d0
    while d <= d1:
        yield d.strftime('%Y%m%d')
        d += timedelta(days=1)


def parse_naver_rows(rows):
    out = defaultdict(list)
    if isinstance(rows, dict):
        rows = rows.get('chartDomesticList') or rows.get('data') or []
    for r in rows or []:
        dtstr = str(r.get('localDateTime') or r.get('localDate') or '')
        if len(dtstr) < 12:
            continue
        ds = dtstr[:8]
        ts = dtstr[8:12]
        try:
            row = {
                'date': ds,
                'time': ts,
                'open': float(r.get('openPrice')),
                'high': float(r.get('highPrice')),
                'low': float(r.get('lowPrice')),
                'close': float(r.get('currentPrice') if r.get('currentPrice') is not None else r.get('closePrice')),
                'volume': float(r.get('accumulatedTradingVolume') or r.get('volume') or 0),
            }
        except (TypeError, ValueError):
            continue
        out[ds].append(row)
    for ds in out:
        out[ds].sort(key=lambda x: x['time'])
    return dict(out)


def fetch_naver_intraday(code: str):
    base = f'https://api.stock.naver.com/chart/domestic/item/{code}/minute10'
    month_url = base + '?' + urllib.parse.urlencode({
        'startDateTime': START + '0900',
        'endDateTime': END + '1530',
    })
    diagnostics = {'mode': 'monthly', 'errors': []}
    try:
        parsed = parse_naver_rows(get_json(month_url))
    except Exception as e:
        parsed = {}
        diagnostics['errors'].append(str(e))
    july_dates = [d for d in parsed if JULY_START <= d <= JULY_END]
    if len(july_dates) >= 10:
        diagnostics['unique_dates'] = len(parsed)
        diagnostics['bars'] = sum(len(v) for v in parsed.values())
        return parsed, diagnostics

    diagnostics['mode'] = 'daily'
    parsed = {}
    for ds in daterange(START, END):
        d = datetime.strptime(ds, '%Y%m%d').date()
        if d.weekday() >= 5:
            continue
        url = base + '?' + urllib.parse.urlencode({
            'startDateTime': ds + '0900',
            'endDateTime': ds + '1530',
        })
        try:
            day = parse_naver_rows(get_json(url, retries=3))
            if ds in day:
                parsed[ds] = day[ds]
        except Exception as e:
            diagnostics['errors'].append(f'{ds}: {e}')
        time.sleep(0.08)
    diagnostics['unique_dates'] = len(parsed)
    diagnostics['bars'] = sum(len(v) for v in parsed.values())
    return parsed, diagnostics


def fetch_yahoo_intraday(symbol: str):
    p1 = int(datetime(2026, 6, 29, 0, 0, tzinfo=KST).timestamp())
    p2 = int(datetime(2026, 7, 30, 0, 0, tzinfo=KST).timestamp())
    params = {
        'period1': p1,
        'period2': p2,
        'interval': '5m',
        'includePrePost': 'false',
        'events': 'div,splits',
    }
    last = None
    payload = None
    used_host = None
    for host in ('query1.finance.yahoo.com', 'query2.finance.yahoo.com'):
        url = f'https://{host}/v8/finance/chart/{urllib.parse.quote(symbol, safe="")}?{urllib.parse.urlencode(params)}'
        try:
            payload = get_json(url, retries=4)
            result = ((payload or {}).get('chart') or {}).get('result') or []
            if result:
                used_host = host
                break
        except Exception as e:
            last = e
    if not payload or not (((payload or {}).get('chart') or {}).get('result') or []):
        raise RuntimeError(f'Yahoo no data for {symbol}: {last or payload}')
    result = payload['chart']['result'][0]
    ts = result.get('timestamp') or []
    quote = ((result.get('indicators') or {}).get('quote') or [{}])[0]
    fields = {k: quote.get(k) or [] for k in ('open', 'high', 'low', 'close', 'volume')}
    out = defaultdict(list)
    for i, epoch in enumerate(ts):
        vals = {k: (fields[k][i] if i < len(fields[k]) else None) for k in fields}
        if any(vals[k] is None for k in ('open', 'high', 'low', 'close')):
            continue
        dt = datetime.fromtimestamp(int(epoch), tz=KST)
        ds, hm = dt.strftime('%Y%m%d'), dt.strftime('%H%M')
        if hm < '0850' or hm > '1540':
            continue
        out[ds].append({
            'date': ds,
            'time': hm,
            'open': float(vals['open']),
            'high': float(vals['high']),
            'low': float(vals['low']),
            'close': float(vals['close']),
            'volume': float(vals['volume'] or 0),
        })
    for ds in out:
        out[ds].sort(key=lambda x: x['time'])
    return dict(out), {
        'host': used_host,
        'symbol': symbol,
        'exchange_timezone': (result.get('meta') or {}).get('exchangeTimezoneName'),
        'unique_dates': len(out),
        'bars': sum(len(v) for v in out.values()),
    }


def weighted_price(rows):
    if not rows:
        return None
    weights = [max(0.0, float(r.get('volume') or 0)) for r in rows]
    prices = [float(r['close']) for r in rows]
    sw = sum(weights)
    if sw > 0:
        return sum(p * w for p, w in zip(prices, weights)) / sw
    return statistics.fmean(prices)


def build_daily(bar_days: dict, asset_key: str, source: str):
    dates = sorted(d for d, rows in bar_days.items() if rows)
    all_summaries = []
    prev_close = None
    for ds in dates:
        rows = sorted(bar_days[ds], key=lambda x: x['time'])
        regular = [r for r in rows if '0900' <= r['time'] <= '1530']
        if not regular:
            continue
        first, last = regular[0], regular[-1]
        open_px = float(first['open'])
        close_px = float(last['close'])
        day_high = max(float(r['high']) for r in regular)
        day_low = min(float(r['low']) for r in regular)
        high_rows = [r for r in regular if abs(float(r['high']) - day_high) <= max(1e-9, abs(day_high) * 1e-10)]
        low_rows = [r for r in regular if abs(float(r['low']) - day_low) <= max(1e-9, abs(day_low) * 1e-10)]
        windows = {}
        for label, start, end in (
            ('10:00-10:30', '1000', '1030'),
            ('12:00-12:30', '1200', '1230'),
            ('14:00-14:30', '1400', '1430'),
        ):
            wr = [r for r in regular if start <= r['time'] < end]
            windows[label] = weighted_price(wr)
        rec = {
            'date': ds,
            'asset': asset_key,
            'source': source,
            'bar_count': len(regular),
            'open': open_px,
            '10:00-10:30': windows['10:00-10:30'],
            '12:00-12:30': windows['12:00-12:30'],
            '14:00-14:30': windows['14:00-14:30'],
            'close': close_px,
            'high': day_high,
            'low': day_low,
            'high_time': high_rows[0]['time'] if high_rows else None,
            'low_time': low_rows[0]['time'] if low_rows else None,
            'previous_close': prev_close,
            'return_prev_pct': ((close_px / prev_close - 1) * 100) if prev_close else None,
            'return_open_pct': (close_px / open_px - 1) * 100,
            'open_gap_pct': ((open_px / prev_close - 1) * 100) if prev_close else None,
            'open_is_exact_high': abs(open_px - day_high) <= max(1e-9, abs(day_high) * 1e-10),
            'open_within_0_3pct_of_high': (day_high / open_px - 1) <= 0.0030000001 if open_px else False,
        }
        rec['complete_slots'] = all(rec.get(k) is not None for k in ('10:00-10:30', '12:00-12:30', '14:00-14:30'))
        if open_px:
            for k in ('open', '10:00-10:30', '12:00-12:30', '14:00-14:30', 'close'):
                rec[k + '_index'] = rec[k] / open_px * 100 if rec.get(k) is not None else None
        all_summaries.append(rec)
        prev_close = close_px
    return all_summaries


def bucket_time(hm):
    if not hm:
        return '미상'
    if hm <= '1030':
        return '09:00~10:30'
    if hm <= '1230':
        return '10:31~12:30'
    if hm <= '1430':
        return '12:31~14:30'
    return '14:31~종가'


def clean(xs):
    return [float(x) for x in xs if x is not None and math.isfinite(float(x))]


def mean(xs):
    xs = clean(xs)
    return statistics.fmean(xs) if xs else None


def median(xs):
    xs = clean(xs)
    return statistics.median(xs) if xs else None


def percentile(xs, p):
    xs = sorted(clean(xs))
    if not xs:
        return None
    if len(xs) == 1:
        return xs[0]
    pos = (len(xs) - 1) * p
    lo = int(math.floor(pos)); hi = int(math.ceil(pos))
    if lo == hi:
        return xs[lo]
    return xs[lo] + (xs[hi] - xs[lo]) * (pos - lo)


def summarize_subset(rows, label):
    slots = ['open', '10:00-10:30', '12:00-12:30', '14:00-14:30', 'close']
    complete = [r for r in rows if r.get('complete_slots')]
    summary = {
        'label': label,
        'n': len(rows),
        'n_complete': len(complete),
        'raw_mean': {k: mean(r.get(k) for r in complete) for k in slots},
        'raw_median': {k: median(r.get(k) for r in complete) for k in slots},
        'open100_mean': {k: mean(r.get(k + '_index') for r in complete) for k in slots},
        'open100_median': {k: median(r.get(k + '_index') for r in complete) for k in slots},
        'open100_p25': {k: percentile((r.get(k + '_index') for r in complete), 0.25) for k in slots},
        'open100_p75': {k: percentile((r.get(k + '_index') for r in complete), 0.75) for k in slots},
        'segment_return_mean_pct': {},
        'segment_return_median_pct': {},
        'open_exact_high_count': sum(bool(r.get('open_is_exact_high')) for r in rows),
        'open_near_high_0_3_count': sum(bool(r.get('open_within_0_3pct_of_high')) for r in rows),
        'low_time_bucket_counts': dict(Counter(bucket_time(r.get('low_time')) for r in rows)),
        'high_time_bucket_counts': dict(Counter(bucket_time(r.get('high_time')) for r in rows)),
        'mean_return_prev_pct': mean(r.get('return_prev_pct') for r in rows),
        'median_return_prev_pct': median(r.get('return_prev_pct') for r in rows),
        'mean_return_open_pct': mean(r.get('return_open_pct') for r in rows),
        'median_return_open_pct': median(r.get('return_open_pct') for r in rows),
        'mean_open_gap_pct': mean(r.get('open_gap_pct') for r in rows),
        'median_open_gap_pct': median(r.get('open_gap_pct') for r in rows),
    }
    for a, b in zip(slots[:-1], slots[1:]):
        vals = [((r[b] / r[a] - 1) * 100) for r in complete if r.get(a) and r.get(b)]
        key = f'{a}->{b}'
        summary['segment_return_mean_pct'][key] = mean(vals)
        summary['segment_return_median_pct'][key] = median(vals)
    return summary


def pearson(xs, ys):
    pairs = [(float(x), float(y)) for x, y in zip(xs, ys) if x is not None and y is not None and math.isfinite(float(x)) and math.isfinite(float(y))]
    if len(pairs) < 3:
        return None
    a, b = zip(*pairs)
    ma, mb = statistics.fmean(a), statistics.fmean(b)
    num = sum((x-ma)*(y-mb) for x, y in pairs)
    da = math.sqrt(sum((x-ma)**2 for x in a)); db = math.sqrt(sum((y-mb)**2 for y in b))
    return num/(da*db) if da and db else None


def validation(primary_rows, yahoo_rows):
    p = {r['date']: r for r in primary_rows}
    y = {r['date']: r for r in yahoo_rows}
    dates = sorted(set(p) & set(y))
    open_bps = []
    close_bps = []
    for ds in dates:
        if p[ds]['open'] and y[ds]['open']:
            open_bps.append(abs(p[ds]['open']/y[ds]['open'] - 1) * 10000)
        if p[ds]['close'] and y[ds]['close']:
            close_bps.append(abs(p[ds]['close']/y[ds]['close'] - 1) * 10000)
    return {
        'overlap_dates': len(dates),
        'mean_abs_open_diff_bps': mean(open_bps),
        'max_abs_open_diff_bps': max(open_bps) if open_bps else None,
        'mean_abs_close_diff_bps': mean(close_bps),
        'max_abs_close_diff_bps': max(close_bps) if close_bps else None,
    }


def main():
    result = {
        'generated_at_kst': datetime.now(KST).isoformat(),
        'analysis_period': {'start': JULY_START, 'end': JULY_END},
        'definitions': {
            'stocks_windows': 'Naver 10-minute bars; close-price weighted by bar trading volume over [start,end).',
            'index_windows': 'Yahoo 5-minute bars; volume-weighted if nonzero, otherwise time-weighted mean over [start,end).',
            'down_prev': 'close below previous trading-day close',
            'intraday_down': 'close below same-day open',
            'stress_dates': sorted(STRESS_DATES),
            'open_near_high': 'day high is no more than 0.3% above open',
        },
        'assets': {},
        'validations': {},
        'correlations': {},
        'diagnostics': {},
    }

    naver_series = {}
    for code in ('005930', '000660', '069500'):
        bars, diag = fetch_naver_intraday(code)
        naver_series[code] = bars
        result['diagnostics'][f'naver_{code}'] = diag

    yahoo_series = {}
    for key, symbol in (('KOSPI200', '^KS200'), ('005930', '005930.KS'), ('000660', '000660.KS')):
        try:
            bars, diag = fetch_yahoo_intraday(symbol)
            yahoo_series[key] = bars
            result['diagnostics'][f'yahoo_{key}'] = diag
        except Exception as e:
            result['diagnostics'][f'yahoo_{key}'] = {'error': str(e)}

    index_source = 'Yahoo ^KS200 5m'
    index_bars = yahoo_series.get('KOSPI200') or {}
    if len([d for d in index_bars if JULY_START <= d <= JULY_END]) < 10:
        index_bars = naver_series.get('069500') or {}
        index_source = 'Naver KODEX 200 10m proxy (actual index unavailable)'

    primary_bars = {
        'KOSPI200': (index_bars, index_source),
        '005930': (naver_series.get('005930') or yahoo_series.get('005930') or {}, 'Naver 10m' if naver_series.get('005930') else 'Yahoo 5m fallback'),
        '000660': (naver_series.get('000660') or yahoo_series.get('000660') or {}, 'Naver 10m' if naver_series.get('000660') else 'Yahoo 5m fallback'),
    }

    daily_by_asset = {}
    for key, (bars, source) in primary_bars.items():
        rows_all = build_daily(bars, key, source)
        rows = [r for r in rows_all if JULY_START <= r['date'] <= JULY_END]
        daily_by_asset[key] = rows
        down_prev = [r for r in rows if r.get('return_prev_pct') is not None and r['return_prev_pct'] < 0]
        intraday_down = [r for r in rows if r.get('return_open_pct') is not None and r['return_open_pct'] < 0]
        nonstress = [r for r in rows if r['date'] not in STRESS_DATES]
        down_prev_nonstress = [r for r in down_prev if r['date'] not in STRESS_DATES]
        stress = [r for r in rows if r['date'] in STRESS_DATES]
        result['assets'][key] = {
            'name': ASSETS[key]['name'],
            'source': source,
            'trading_dates': [r['date'] for r in rows],
            'daily': rows,
            'summaries': {
                'all_days': summarize_subset(rows, 'all_days'),
                'down_vs_prev_close': summarize_subset(down_prev, 'down_vs_prev_close'),
                'intraday_down_close_below_open': summarize_subset(intraday_down, 'intraday_down_close_below_open'),
                'excluding_20260728_29': summarize_subset(nonstress, 'excluding_20260728_29'),
                'down_vs_prev_excluding_20260728_29': summarize_subset(down_prev_nonstress, 'down_vs_prev_excluding_20260728_29'),
                'stress_20260728_29': summarize_subset(stress, 'stress_20260728_29'),
            },
        }

    for key in ('005930', '000660'):
        if yahoo_series.get(key):
            p = [r for r in build_daily(naver_series.get(key) or {}, key, 'Naver 10m') if JULY_START <= r['date'] <= JULY_END]
            y = [r for r in build_daily(yahoo_series[key], key, 'Yahoo 5m') if JULY_START <= r['date'] <= JULY_END]
            result['validations'][key] = validation(p, y)

    if naver_series.get('069500') and yahoo_series.get('KOSPI200'):
        idx = {r['date']: r for r in build_daily(yahoo_series['KOSPI200'], 'KOSPI200', 'Yahoo') if JULY_START <= r['date'] <= JULY_END}
        etf = {r['date']: r for r in build_daily(naver_series['069500'], '069500', 'Naver') if JULY_START <= r['date'] <= JULY_END}
        dates = sorted(set(idx) & set(etf))
        result['validations']['KOSPI200_vs_KODEX200'] = {
            'overlap_dates': len(dates),
            'corr_intraday_close_open_return': pearson([idx[d]['return_open_pct'] for d in dates], [etf[d]['return_open_pct'] for d in dates]),
            'mean_abs_return_diff_pct_point': mean(abs(idx[d]['return_open_pct'] - etf[d]['return_open_pct']) for d in dates),
        }

    keys = list(daily_by_asset)
    for i, a in enumerate(keys):
        for b in keys[i+1:]:
            da = {r['date']: r for r in daily_by_asset[a]}
            db = {r['date']: r for r in daily_by_asset[b]}
            dates = sorted(set(da) & set(db))
            result['correlations'][f'{a}__{b}'] = {
                'n': len(dates),
                'daily_return_prev_corr': pearson([da[d]['return_prev_pct'] for d in dates], [db[d]['return_prev_pct'] for d in dates]),
                'intraday_close_open_corr': pearson([da[d]['return_open_pct'] for d in dates], [db[d]['return_open_pct'] for d in dates]),
                'open_gap_corr': pearson([da[d]['open_gap_pct'] for d in dates], [db[d]['open_gap_pct'] for d in dates]),
            }

    with open('tmp_intraday_results.json', 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    lines = ['# 2026-07 Korean intraday analysis diagnostics', '', f"Generated: {result['generated_at_kst']}", '']
    for key, asset in result['assets'].items():
        s = asset['summaries']['all_days']
        d = asset['summaries']['down_vs_prev_close']
        lines += [
            f"## {asset['name']} ({key})",
            f"- source: {asset['source']}",
            f"- trading days: {s['n']} / complete: {s['n_complete']}",
            f"- down days: {d['n']}",
            f"- all-day open100 mean: {s['open100_mean']}",
            f"- down-day open100 mean: {d['open100_mean']}",
            f"- down-day near-open-high: {d['open_near_high_0_3_count']}/{d['n']}",
            f"- down-day low buckets: {d['low_time_bucket_counts']}",
            '',
        ]
    lines += ['## Validations', '```json', json.dumps(result['validations'], ensure_ascii=False, indent=2), '```', '', '## Diagnostics', '```json', json.dumps(result['diagnostics'], ensure_ascii=False, indent=2), '```']
    with open('tmp_intraday_results.md', 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))


if __name__ == '__main__':
    main()

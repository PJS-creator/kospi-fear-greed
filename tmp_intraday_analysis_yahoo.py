import tmp_intraday_analysis as analysis


def no_naver(code: str):
    return {}, {"mode": "disabled_for_full_month_consistency", "unique_dates": 0, "bars": 0, "errors": []}


analysis.fetch_naver_intraday = no_naver
analysis.main()

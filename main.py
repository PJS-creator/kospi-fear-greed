import os
import re
import csv
import time
import random
from dataclasses import dataclass
from datetime import datetime, date, time as dtime
from typing import Optional, Tuple, List, Dict, Set
from urllib.parse import urlsplit, urlunsplit, parse_qs, urlencode, urljoin

import requests
from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo

# =========================
# 기본 설정
# =========================
KST = ZoneInfo("Asia/Seoul")

BASE_LIST_URL = "https://gall.dcinside.com/mgallery/board/lists/"
BOARD_ID = os.getenv("BOARD_ID", "kospi")
OUT_PREFIX = os.getenv("OUT_PREFIX", BOARD_ID)

# 수집 시간대 (KST)
START_TIME_STR = os.getenv("START_TIME", "08:50")
END_TIME_STR = os.getenv("END_TIME", "15:40")

# 특정 날짜만 단일 실행하고 싶으면 (YYYY-MM-DD). 비우면 date.txt 전체 실행
TARGET_DATE_STR = os.getenv("TARGET_DATE", "").strip()

# 날짜 파일 (각 줄 첫 토큰이 YYYY-MM-DD 라면 OK)
DATES_FILE = os.getenv("DATES_FILE", "date.txt")

# 페이지/부하 제어
LIST_NUM = int(os.getenv("LIST_NUM", "100"))
MAX_PAGE_LIMIT = int(os.getenv("MAX_PAGE_LIMIT", "20000"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "20"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))
SLEEP_LIST = float(os.getenv("SLEEP_LIST", "0.7"))
SLEEP_POST = float(os.getenv("SLEEP_POST", "0.45"))
OUT_DIR = os.getenv("OUT_DIR", "outputs")

# kospi 같이 활발한 갤에서 경계 페이지를 안정적으로 포착하기 위한 여유값
TARGET_PAGE_SEARCH_RADIUS = int(os.getenv("TARGET_PAGE_SEARCH_RADIUS", "80"))
PAGE_WINDOW_MARGIN = int(os.getenv("PAGE_WINDOW_MARGIN", "25"))
PAGE_WINDOW_MAX_EXPAND = int(os.getenv("PAGE_WINDOW_MAX_EXPAND", "200"))

USER_AGENT = os.getenv(
    "USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
)

DEFAULT_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}


# =========================
# 데이터 구조
# =========================
@dataclass(frozen=True)
class PostRow:
    post_no: int
    head: str
    title: str
    url: str
    date_text: str
    date_attr: Optional[str]
    is_notice: bool


# =========================
# 유틸
# =========================
def parse_hhmm(s: str) -> dtime:
    return datetime.strptime(s, "%H:%M").time()


def jitter_sleep(base: float) -> None:
    time.sleep(base + random.uniform(0, base * 0.35))


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def clean_title(s: str) -> str:
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"\[\d+\]\s*$", "", s).strip()
    return s


def build_list_url(page: int) -> str:
    parts = urlsplit(BASE_LIST_URL)
    qs = parse_qs(parts.query)
    qs["id"] = [BOARD_ID]
    qs["page"] = [str(page)]
    qs["list_num"] = [str(LIST_NUM)]
    new_query = urlencode(qs, doseq=True)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, new_query, parts.fragment))


def make_session() -> requests.Session:
    sess = requests.Session()
    sess.headers.update(DEFAULT_HEADERS)
    return sess


def fetch_html(session: requests.Session, url: str, referer: Optional[str] = None) -> str:
    last_err: Optional[Exception] = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            headers = {}
            if referer:
                headers["Referer"] = referer

            resp = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200 and resp.text:
                return resp.text

            if resp.status_code in (403, 429, 500, 502, 503, 504):
                last_err = RuntimeError(f"HTTP {resp.status_code}")
                jitter_sleep(1.2 * attempt)
                continue

            last_err = RuntimeError(f"HTTP {resp.status_code}")
            jitter_sleep(0.8 * attempt)

        except Exception as e:
            last_err = e
            jitter_sleep(1.2 * attempt)

    raise RuntimeError(f"요청 실패: {url} / 마지막 에러: {last_err}")


def extract_open_date_from_list(html: str) -> Optional[date]:
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(" ", strip=True)
    m = re.search(r"개설일\s*(\d{4}-\d{2}-\d{2})", text)
    if not m:
        return None
    try:
        return date.fromisoformat(m.group(1))
    except ValueError:
        return None


def _pick_main_table(soup: BeautifulSoup):
    tables = soup.find_all("table")
    for t in tables:
        thead = t.find("thead")
        thead_text = thead.get_text(" ", strip=True) if thead else t.get_text(" ", strip=True)[:250]
        if ("제목" in thead_text) and ("작성일" in thead_text) and ("글쓴이" in thead_text):
            return t
    return tables[0] if tables else None


def extract_rows(html: str, page_url: str) -> List[PostRow]:
    soup = BeautifulSoup(html, "lxml")
    table = _pick_main_table(soup)
    if not table:
        return []

    tbody = table.find("tbody") or table
    trs = tbody.find_all("tr")
    rows: List[PostRow] = []

    for tr in trs:
        tds = tr.find_all("td")
        if len(tds) < 6:
            continue

        num_text = tds[0].get_text(strip=True)
        if not num_text.isdigit():
            continue

        post_no = int(num_text)

        head = tds[1].get_text(" ", strip=True)
        is_notice = head.strip() == "공지"

        title_td = tds[2]
        a = None
        for cand in title_td.find_all("a", href=True):
            if "board/view" in cand["href"]:
                a = cand
                break
        if a is None:
            a = title_td.find("a", href=True)

        if not a or not a.get("href"):
            continue

        title = clean_title(a.get_text(" ", strip=True))
        if not title:
            continue

        url = urljoin(page_url, a["href"])

        date_td = tds[4]
        date_text = date_td.get_text(" ", strip=True)

        date_attr = None
        if date_td.has_attr("title"):
            date_attr = str(date_td.get("title", "")).strip() or None
        else:
            span_with_title = date_td.find(attrs={"title": True})
            if span_with_title:
                date_attr = str(span_with_title.get("title", "")).strip() or None

        rows.append(
            PostRow(
                post_no=post_no,
                head=head,
                title=title,
                url=url,
                date_text=date_text,
                date_attr=date_attr,
                is_notice=is_notice,
            )
        )

    return rows


def parse_full_datetime(s: str) -> Optional[datetime]:
    s = s.strip()
    m = re.fullmatch(
        r"(\d{4})[.\-](\d{2})[.\-](\d{2})\s+([0-2]\d):([0-5]\d)(?::([0-5]\d))?",
        s,
    )
    if not m:
        return None
    y, mo, d, hh, mm, ss = m.groups()
    sec = int(ss) if ss is not None else 0
    try:
        return datetime(int(y), int(mo), int(d), int(hh), int(mm), sec, tzinfo=KST)
    except ValueError:
        return None


def parse_list_date_or_datetime(
    date_text: str, date_attr: Optional[str], now_kst: datetime
) -> Tuple[Optional[datetime], Optional[date]]:
    if date_attr:
        dt = parse_full_datetime(date_attr)
        if dt:
            return dt, None

    s = re.sub(r"\s+", " ", date_text.strip())

    if re.fullmatch(r"\d{1,2}:\d{2}", s):
        h, m = map(int, s.split(":"))
        return datetime.combine(now_kst.date(), dtime(h, m), tzinfo=KST), None

    if re.fullmatch(r"\d{2}\.\d{2}\.\d{2}", s):
        yy, mo, d = map(int, s.split("."))
        return None, date(2000 + yy, mo, d)

    if re.fullmatch(r"\d{4}\.\d{2}\.\d{2}", s):
        try:
            return None, datetime.strptime(s, "%Y.%m.%d").date()
        except ValueError:
            return None, None

    if re.fullmatch(r"\d{2}\.\d{2}", s):
        mo, d = map(int, s.split("."))
        y = now_kst.year
        try:
            candidate = date(y, mo, d)
        except ValueError:
            return None, None
        if candidate > now_kst.date():
            candidate = date(y - 1, mo, d)
        return None, candidate

    return None, None


def parse_datetime_from_post(html: str, preferred_date: Optional[date] = None) -> Optional[datetime]:
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(" ", strip=True)

    candidates: List[datetime] = []
    for m in re.finditer(
        r"(\d{4})[.\-](\d{2})[.\-](\d{2})\s+([0-2]\d):([0-5]\d)(?::([0-5]\d))?",
        text,
    ):
        y, mo, d, hh, mm, ss = m.groups()
        sec = int(ss) if ss is not None else 0
        try:
            dt = datetime(int(y), int(mo), int(d), int(hh), int(mm), sec, tzinfo=KST)
            candidates.append(dt)
        except ValueError:
            continue

    if preferred_date is not None:
        for dt in candidates:
            if dt.date() == preferred_date:
                return dt

    return candidates[0] if candidates else None


def load_dates_from_file(path: str) -> List[date]:
    dates: List[date] = []
    if not os.path.exists(path):
        raise FileNotFoundError(f"날짜 파일을 찾지 못했습니다: {path}")

    with open(path, "r", encoding="utf-8-sig") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            if not re.match(r"^\d{4}-\d{2}-\d{2}\b", line):
                continue
            token = line.split()[0]
            try:
                dates.append(date.fromisoformat(token))
            except ValueError:
                continue

    return sorted(set(dates))


def write_csv(out_path: str, rows: List[Tuple[datetime, str, str]]) -> None:
    rows_sorted = sorted(rows, key=lambda x: x[0])
    with open(out_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["작성시간(KST)", "제목", "URL"])
        for dt, title, url in rows_sorted:
            w.writerow([dt.strftime("%Y-%m-%d %H:%M:%S"), title, url])


def get_page_date_range(
    session: requests.Session,
    page: int,
    now_kst: datetime,
    cache: Dict[int, Tuple[Optional[date], Optional[date]]],
) -> Tuple[Optional[date], Optional[date]]:
    """
    페이지에서 (가장 최신 날짜, 가장 오래된 날짜)를 반환한다.
    순서가 약간 흔들리더라도 max/min으로 계산해 경계 탐색을 안정화한다.
    """
    if page in cache:
        return cache[page]

    url = build_list_url(page)
    html = fetch_html(session, url)
    rows = extract_rows(html, url)

    d_list: List[date] = []
    for r in rows:
        if r.is_notice:
            continue
        dt, d = parse_list_date_or_datetime(r.date_text, r.date_attr, now_kst)
        row_date = dt.date() if dt else d
        if row_date:
            d_list.append(row_date)

    if not d_list:
        cache[page] = (None, None)
        return None, None

    newest = max(d_list)
    oldest = min(d_list)
    cache[page] = (newest, oldest)
    return newest, oldest


def page_intersects_target(
    session: requests.Session,
    page: int,
    target: date,
    now_kst: datetime,
    cache: Dict[int, Tuple[Optional[date], Optional[date]]],
) -> bool:
    newest, oldest = get_page_date_range(session, page, now_kst, cache)
    if newest is None or oldest is None:
        return False
    return newest >= target >= oldest


def find_upper_bound_for_min_date(
    session: requests.Session, min_date: date, now_kst: datetime
) -> Tuple[int, Dict[int, Tuple[Optional[date], Optional[date]]]]:
    cache: Dict[int, Tuple[Optional[date], Optional[date]]] = {}

    hi = 1
    while hi <= MAX_PAGE_LIMIT:
        _newest, oldest = get_page_date_range(session, hi, now_kst, cache)
        if oldest is None:
            return hi, cache
        if oldest <= min_date:
            return hi, cache
        hi *= 2

    return MAX_PAGE_LIMIT, cache


def find_start_page_for_date(
    session: requests.Session,
    target: date,
    now_kst: datetime,
    hi: int,
    cache: Dict[int, Tuple[Optional[date], Optional[date]]],
) -> int:
    """
    oldest(page) <= target 를 만족하는 가장 작은 page를 이진탐색.
    실제 수집은 이 값을 anchor 로 쓰고, 주변에서 target이 걸치는 실제 window를 다시 찾는다.
    """
    lo = 1
    r = hi
    ans = hi

    while lo <= r:
        mid = (lo + r) // 2
        _newest, oldest = get_page_date_range(session, mid, now_kst, cache)

        if oldest is None:
            ans = mid
            r = mid - 1
            continue

        if oldest <= target:
            ans = mid
            r = mid - 1
        else:
            lo = mid + 1

    return ans


def find_target_window(
    session: requests.Session,
    target: date,
    now_kst: datetime,
    anchor_page: int,
    cache: Dict[int, Tuple[Optional[date], Optional[date]]],
) -> Tuple[int, int]:
    """
    anchor_page 주변에서 target 날짜가 실제로 걸치는 page window를 찾는다.
    1) anchor 주변에서 target intersect page를 탐색
    2) hit를 찾으면 좌우로 확장
    """
    hit_page: Optional[int] = None
    max_radius = min(PAGE_WINDOW_MAX_EXPAND, MAX_PAGE_LIMIT)

    for radius in range(0, max_radius + 1):
        candidates = []
        if radius == 0:
            candidates = [anchor_page]
        else:
            left = anchor_page - radius
            right = anchor_page + radius
            if left >= 1:
                candidates.append(left)
            if right <= MAX_PAGE_LIMIT:
                candidates.append(right)

        for page in candidates:
            if page_intersects_target(session, page, target, now_kst, cache):
                hit_page = page
                break

        if hit_page is not None and radius >= TARGET_PAGE_SEARCH_RADIUS:
            break
        if hit_page is not None and radius >= 2:
            break

    if hit_page is None:
        # 비정상 케이스: intersect page를 못 찾으면 anchor 주변 넓은 구간을 그냥 스캔
        left = max(1, anchor_page - PAGE_WINDOW_MARGIN)
        right = min(MAX_PAGE_LIMIT, anchor_page + PAGE_WINDOW_MARGIN)
        return left, right

    left = hit_page
    while left > 1 and page_intersects_target(session, left - 1, target, now_kst, cache):
        left -= 1

    right = hit_page
    while right < MAX_PAGE_LIMIT and page_intersects_target(session, right + 1, target, now_kst, cache):
        right += 1

    return left, right


def collect_candidate_rows_for_date(
    session: requests.Session,
    target: date,
    now_kst: datetime,
    page_from: int,
    page_to: int,
) -> List[Tuple[PostRow, str, Optional[datetime]]]:
    """
    target 날짜가 들어 있을 법한 page window를 넓게 훑어서 후보를 모두 모은다.
    조기 종료를 하지 않고, 지정 구간 전체를 스캔한 뒤 post_no로 dedupe 한다.
    """
    candidates: List[Tuple[PostRow, str, Optional[datetime]]] = []
    seen_post_no: Set[int] = set()

    for page in range(max(1, page_from), min(MAX_PAGE_LIMIT, page_to) + 1):
        page_url = build_list_url(page)
        html = fetch_html(session, page_url)
        rows = extract_rows(html, page_url)
        if not rows:
            continue

        for r in rows:
            if r.is_notice:
                continue

            dt_guess, d_guess = parse_list_date_or_datetime(r.date_text, r.date_attr, now_kst)
            row_date = dt_guess.date() if dt_guess else d_guess
            if row_date != target:
                continue

            if r.post_no in seen_post_no:
                continue
            seen_post_no.add(r.post_no)
            candidates.append((r, page_url, dt_guess))

        jitter_sleep(SLEEP_LIST)

    candidates.sort(key=lambda x: x[0].post_no, reverse=True)
    return candidates


def scrape_one_date(
    session: requests.Session,
    target: date,
    start_t: dtime,
    end_t: dtime,
    now_kst: datetime,
    page_from: int,
    page_to: int,
) -> List[Tuple[datetime, str, str]]:
    results: List[Tuple[datetime, str, str]] = []
    post_dt_cache: Dict[str, datetime] = {}

    candidates = collect_candidate_rows_for_date(
        session=session,
        target=target,
        now_kst=now_kst,
        page_from=page_from,
        page_to=page_to,
    )
    print(f"[INFO] {target} 후보 글 {len(candidates)}건 (scan_pages={page_from}..{page_to})")

    for idx, (r, referer_page_url, dt_guess) in enumerate(candidates, start=1):
        if dt_guess is None:
            if r.url in post_dt_cache:
                dt = post_dt_cache[r.url]
            else:
                jitter_sleep(SLEEP_POST)
                post_html = fetch_html(session, r.url, referer=referer_page_url)
                dt = parse_datetime_from_post(post_html, preferred_date=target)
                if dt is None:
                    continue
                post_dt_cache[r.url] = dt
        else:
            dt = dt_guess

        t = dt.timetz().replace(tzinfo=None)
        if start_t <= t <= end_t:
            results.append((dt, r.title, r.url))

        if idx % 100 == 0:
            print(f"[INFO] {target} 상세 확인 진행 {idx}/{len(candidates)}")

    results.sort(key=lambda x: x[0])
    return results


def main():
    now_kst = datetime.now(KST)
    start_t = parse_hhmm(START_TIME_STR)
    end_t = parse_hhmm(END_TIME_STR)
    ensure_dir(OUT_DIR)

    if TARGET_DATE_STR:
        target_dates = [date.fromisoformat(TARGET_DATE_STR)]
        requested_dates = target_dates[:]
    else:
        requested_dates = load_dates_from_file(DATES_FILE)
        target_dates = requested_dates[:]

    if not requested_dates:
        print("대상 날짜가 없습니다. (date.txt를 확인하세요)")
        return

    session = make_session()
    first_page_html = fetch_html(session, build_list_url(1))
    open_date = extract_open_date_from_list(first_page_html)

    if open_date:
        target_dates = [d for d in target_dates if d >= open_date]
        skipped = sorted(set(requested_dates) - set(target_dates))
        if skipped:
            print(f"[INFO] 갤러리 개설일({open_date}) 이전 날짜는 글이 없으므로 빈 CSV로 처리합니다. 예: {skipped[:5]}")

    all_results_by_date: Dict[date, List[Tuple[datetime, str, str]]] = {d: [] for d in requested_dates}

    if not target_dates:
        for d in requested_dates:
            out_path = os.path.join(OUT_DIR, f"{OUT_PREFIX}_{d.isoformat()}.csv")
            write_csv(out_path, [])
        combined_path = os.path.join(OUT_DIR, f"{OUT_PREFIX}_all.csv")
        with open(combined_path, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.writer(f)
            w.writerow(["작성시간(KST)", "날짜", "제목", "URL"])
        print("[DONE] 모든 날짜가 개설일 이전이라 빈 CSV만 생성했습니다.")
        return

    for target in sorted(target_dates, reverse=True):
        fresh_now_kst = datetime.now(KST)
        hi, cache = find_upper_bound_for_min_date(session, target, fresh_now_kst)
        anchor_page = find_start_page_for_date(session, target, fresh_now_kst, hi, cache)
        win_left, win_right = find_target_window(session, target, fresh_now_kst, anchor_page, cache)
        scan_from = max(1, win_left - PAGE_WINDOW_MARGIN)
        scan_to = min(MAX_PAGE_LIMIT, win_right + PAGE_WINDOW_MARGIN)

        print(
            f"\n=== {target} anchor={anchor_page}, target_window={win_left}..{win_right}, "
            f"scan={scan_from}..{scan_to}, hi={hi} ==="
        )

        rows = scrape_one_date(
            session=session,
            target=target,
            start_t=start_t,
            end_t=end_t,
            now_kst=fresh_now_kst,
            page_from=scan_from,
            page_to=scan_to,
        )
        all_results_by_date[target] = rows
        print(f"[OK] {target} 수집 {len(rows)}건")

    for d in requested_dates:
        out_path = os.path.join(OUT_DIR, f"{OUT_PREFIX}_{d.isoformat()}.csv")
        write_csv(out_path, all_results_by_date.get(d, []))

    combined_path = os.path.join(OUT_DIR, f"{OUT_PREFIX}_all.csv")
    combined_rows: List[Tuple[datetime, str, str, str]] = []
    for d in requested_dates:
        for dt, title, url in all_results_by_date.get(d, []):
            combined_rows.append((dt, d.isoformat(), title, url))

    combined_rows.sort(key=lambda x: x[0])
    with open(combined_path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["작성시간(KST)", "날짜", "제목", "URL"])
        for dt, d_str, title, url in combined_rows:
            w.writerow([dt.strftime("%Y-%m-%d %H:%M:%S"), d_str, title, url])

    print("\n[DONE] CSV 생성 완료")
    print(f"- 폴더: {OUT_DIR}")
    print(f"- 합본: {combined_path}")


if __name__ == "__main__":
    main()

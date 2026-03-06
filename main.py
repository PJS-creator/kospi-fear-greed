# main.py
import os
import re
import csv
import time
import random
from collections import defaultdict
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
SLEEP_LIST = float(os.getenv("SLEEP_LIST", "0.25"))
SLEEP_POST = float(os.getenv("SLEEP_POST", "0.20"))
OUT_DIR = os.getenv("OUT_DIR", "outputs")

# 선형 스캔 종료 조건
EDGE_SAMPLE_SIZE = int(os.getenv("EDGE_SAMPLE_SIZE", "20"))
EDGE_OUTLIER_DAYS = int(os.getenv("EDGE_OUTLIER_DAYS", "3"))
STOP_OLDER_CONSECUTIVE_PAGES = int(os.getenv("STOP_OLDER_CONSECUTIVE_PAGES", "10"))
PAGE_PROGRESS_EVERY = int(os.getenv("PAGE_PROGRESS_EVERY", "10"))

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
    if base <= 0:
        return
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


def row_created_date(r: PostRow, now_kst: datetime) -> Tuple[Optional[datetime], Optional[date]]:
    return parse_list_date_or_datetime(r.date_text, r.date_attr, now_kst)


def robust_page_main_range(rows: List[PostRow], now_kst: datetime) -> Tuple[Optional[date], Optional[date]]:
    """
    페이지는 대체로 최신→과거 순이지만, 중간에 오래된 글이 끼어드는 outlier가 존재한다.
    따라서 page 전체 min/max를 쓰지 않고, 상단/하단 edge band의 대표 날짜를 사용한다.
    """
    row_dates: List[date] = []
    for r in rows:
        if r.is_notice:
            continue
        dt_guess, d_guess = row_created_date(r, now_kst)
        rd = dt_guess.date() if dt_guess else d_guess
        if rd is not None:
            row_dates.append(rd)

    if not row_dates:
        return None, None

    sample = max(5, min(EDGE_SAMPLE_SIZE, len(row_dates)))
    top = row_dates[:sample]
    bottom = row_dates[-sample:]

    def _filter_band(ds: List[date]) -> List[date]:
        ords = sorted(d.toordinal() for d in ds)
        median_ord = ords[len(ords) // 2]
        filtered = [d for d in ds if abs(d.toordinal() - median_ord) <= EDGE_OUTLIER_DAYS]
        # 필터가 너무 빡세면 원본 사용
        if len(filtered) < max(3, len(ds) // 2):
            return ds
        return filtered

    top_filtered = _filter_band(top)
    bottom_filtered = _filter_band(bottom)
    newest = max(top_filtered)
    oldest = min(bottom_filtered)

    if newest < oldest:
        newest, oldest = oldest, newest
    return newest, oldest


def scan_pages_for_targets(
    session: requests.Session,
    target_dates: List[date],
    requested_dates: List[date],
    start_t: dtime,
    end_t: dtime,
    now_kst: datetime,
) -> Dict[date, List[Tuple[datetime, str, str]]]:
    results_by_date: Dict[date, List[Tuple[datetime, str, str]]] = {d: [] for d in requested_dates}
    target_set: Set[date] = set(target_dates)
    if not target_dates:
        return results_by_date

    oldest_target = min(target_dates)
    newest_target = max(target_dates)

    seen_post_no_by_date: Dict[date, Set[int]] = defaultdict(set)
    seen_url_by_date: Dict[date, Set[str]] = defaultdict(set)
    post_dt_cache: Dict[str, datetime] = {}
    candidate_count_by_date: Dict[date, int] = defaultdict(int)

    passed_oldest_target_zone = False
    consecutive_pages_strictly_older = 0

    print(
        f"[INFO] 선형 스캔 시작: target_dates={min(target_dates)}..{max(target_dates)}, "
        f"stop_after_older_pages={STOP_OLDER_CONSECUTIVE_PAGES}"
    )

    for page in range(1, MAX_PAGE_LIMIT + 1):
        page_url = build_list_url(page)
        html = fetch_html(session, page_url)
        rows = extract_rows(html, page_url)

        if not rows:
            if passed_oldest_target_zone:
                consecutive_pages_strictly_older += 1
                if consecutive_pages_strictly_older >= STOP_OLDER_CONSECUTIVE_PAGES:
                    print(f"[STOP] 빈 페이지가 연속 발생하여 page={page}에서 종료")
                    break
            jitter_sleep(SLEEP_LIST)
            continue

        page_newest, page_oldest = robust_page_main_range(rows, now_kst)
        if page_newest is not None and page_oldest is not None:
            if page_oldest <= oldest_target:
                passed_oldest_target_zone = True

            if passed_oldest_target_zone and page_newest < oldest_target:
                consecutive_pages_strictly_older += 1
            else:
                consecutive_pages_strictly_older = 0
        else:
            if passed_oldest_target_zone:
                consecutive_pages_strictly_older += 1

        if page == 1 or page % PAGE_PROGRESS_EVERY == 0 or (page_newest and page_newest <= newest_target):
            print(
                f"[PAGE {page}] main_range={page_newest}..{page_oldest} "
                f"older_streak={consecutive_pages_strictly_older}"
            )

        # 이 페이지의 행들 중 target date에 해당하는 글만 후보로 처리
        for r in rows:
            if r.is_notice:
                continue

            dt_guess, d_guess = row_created_date(r, now_kst)
            row_date = dt_guess.date() if dt_guess else d_guess
            if row_date is None or row_date not in target_set:
                continue

            if r.post_no in seen_post_no_by_date[row_date]:
                continue
            if r.url in seen_url_by_date[row_date]:
                continue

            seen_post_no_by_date[row_date].add(r.post_no)
            seen_url_by_date[row_date].add(r.url)
            candidate_count_by_date[row_date] += 1

            dt: Optional[datetime]
            if dt_guess is not None:
                dt = dt_guess
            else:
                if r.url in post_dt_cache:
                    dt = post_dt_cache[r.url]
                else:
                    jitter_sleep(SLEEP_POST)
                    post_html = fetch_html(session, r.url, referer=page_url)
                    dt = parse_datetime_from_post(post_html, preferred_date=row_date)
                    if dt is not None:
                        post_dt_cache[r.url] = dt

            if dt is None:
                continue

            t = dt.timetz().replace(tzinfo=None)
            if start_t <= t <= end_t:
                results_by_date[row_date].append((dt, r.title, r.url))

        if page % PAGE_PROGRESS_EVERY == 0:
            summary = ", ".join(
                f"{d}:{len(results_by_date[d])}/{candidate_count_by_date[d]}"
                for d in sorted(target_dates)
            )
            if summary:
                print(f"[INFO] 누적 수집/후보 @page {page}: {summary}")

        if passed_oldest_target_zone and consecutive_pages_strictly_older >= STOP_OLDER_CONSECUTIVE_PAGES:
            print(
                f"[STOP] page {page} 이후로 대표 날짜가 {oldest_target}보다 오래된 페이지가 "
                f"{STOP_OLDER_CONSECUTIVE_PAGES}연속이라 종료"
            )
            break

        jitter_sleep(SLEEP_LIST)

    for d in sorted(target_dates):
        dedup: Dict[Tuple[datetime, str], str] = {}
        for dt, title, url in results_by_date[d]:
            dedup[(dt, url)] = title
        results_by_date[d] = sorted(
            [(dt, title, url) for (dt, url), title in dedup.items()],
            key=lambda x: x[0],
        )
        print(f"[OK] {d} 후보 {candidate_count_by_date[d]}건 / 최종 {len(results_by_date[d])}건")

    return results_by_date


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

    scanned_results = scan_pages_for_targets(
        session=session,
        target_dates=sorted(target_dates),
        requested_dates=requested_dates,
        start_t=start_t,
        end_t=end_t,
        now_kst=now_kst,
    )
    all_results_by_date.update(scanned_results)

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

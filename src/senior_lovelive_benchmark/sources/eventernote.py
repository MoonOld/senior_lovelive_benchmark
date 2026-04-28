from __future__ import annotations

import re
from collections.abc import Callable, Iterable
from urllib.parse import unquote, urlencode, urlparse

from bs4 import BeautifulSoup, Tag

from senior_lovelive_benchmark.http_client import Fetcher
from senior_lovelive_benchmark.models import EventRecord, SourceLink
from senior_lovelive_benchmark.text_utils import (
    absolute_url,
    clean_text,
    first_match,
    unique_preserving_order,
)

BASE_URL = "https://www.eventernote.com"
EVENT_RE = re.compile(r"^/events/(?P<id>\d+)$")
DEFAULT_KEYWORDS = ("LoveLive", "ラブライブ", "Liella", "Aqours", "虹ヶ咲", "蓮ノ空", "μ's")
MAIN_GROUP_ACTOR_URLS = {
    "Aqours": "https://www.eventernote.com/actors/Aqours/15434/events",
    "μ’s": "https://www.eventernote.com/actors/%CE%BC%E2%80%99s/2809/events",
    "虹ヶ咲学園スクールアイドル同好会": "https://www.eventernote.com/actors/%E8%99%B9%E3%83%B6%E5%92%B2%E5%AD%A6%E5%9C%92%E3%82%B9%E3%82%AF%E3%83%BC%E3%83%AB%E3%82%A2%E3%82%A4%E3%83%89%E3%83%AB%E5%90%8C%E5%A5%BD%E4%BC%9A/31600/events",
    "Liella!": "https://www.eventernote.com/actors/Liella%21/59030/events",
    "蓮ノ空女学院スクールアイドルクラブ": "https://www.eventernote.com/actors/%E8%93%AE%E3%83%8E%E7%A9%BA%E5%A5%B3%E5%AD%A6%E9%99%A2%E3%82%B9%E3%82%AF%E3%83%BC%E3%83%AB%E3%82%A2%E3%82%A4%E3%83%89%E3%83%AB%E3%82%AF%E3%83%A9%E3%83%96/70475/events",
    "いきづらい部！": "https://www.eventernote.com/actors/%E3%81%84%E3%81%8D%E3%81%A5%E3%82%89%E3%81%84%E9%83%A8%EF%BC%81/85084/events",
}
DEFAULT_MAIN_GROUP_ACTOR_URLS = tuple(MAIN_GROUP_ACTOR_URLS.values())


class EventernoteClient:
    def __init__(self, fetcher: Fetcher | None = None) -> None:
        self.fetcher = fetcher or Fetcher()

    def search_events(
        self,
        keywords: Iterable[str] = DEFAULT_KEYWORDS,
        max_pages: int | None = 1,
        include_details: bool = True,
        progress: Callable[[str], None] | None = None,
        continue_on_error: bool = True,
        checkpoint: Callable[[list[EventRecord]], None] | None = None,
        checkpoint_every: int = 50,
        existing_records: list[EventRecord] | None = None,
        max_new_details: int | None = None,
    ) -> list[EventRecord]:
        candidates: dict[str, tuple[str, str, list[str]]] = {}
        keyword_list = list(keywords)
        if progress:
            progress(f"[eventernote] search start keywords={len(keyword_list)} max_pages={max_pages or 'all'}")
        for keyword in keyword_list:
            before = len(candidates)
            if progress:
                progress(f"[eventernote] keyword start: {keyword}")
            for event_id, url, title in self._iter_search_results(
                keyword,
                max_pages=max_pages,
                progress=progress,
                continue_on_error=continue_on_error,
            ):
                existing = candidates.get(event_id)
                if existing:
                    existing[2].append(keyword)
                    continue
                candidates[event_id] = (url, title, [keyword])
            if progress:
                progress(f"[eventernote] keyword done: {keyword} new={len(candidates) - before} total={len(candidates)}")

        records: list[EventRecord] = list(existing_records or [])
        existing_by_id = {record.source_id: record for record in records}
        total = len(candidates)
        if progress:
            progress(
                f"[eventernote] detail start candidates={total} cached={len(existing_by_id)} "
                f"include_details={include_details} max_new_details={max_new_details or 'all'}"
            )
        fetched_details = 0
        for index, (event_id, (url, title, matched_keywords)) in enumerate(candidates.items(), start=1):
            if progress and (index == 1 or index % 10 == 0 or index == total):
                progress(f"[eventernote] detail {index}/{total} event_id={event_id}")
            existing_record = existing_by_id.get(event_id)
            if existing_record:
                existing_record.keywords = unique_preserving_order([*existing_record.keywords, *matched_keywords])
                continue
            if max_new_details is not None and fetched_details >= max_new_details:
                if progress:
                    progress(f"[eventernote] detail limit reached max_new_details={max_new_details}")
                break
            if include_details:
                try:
                    record = self.fetch_event_detail(url)
                    record.keywords = unique_preserving_order(matched_keywords)
                except Exception as exc:
                    if not continue_on_error:
                        raise
                    if progress:
                        progress(f"[eventernote] detail error event_id={event_id} error={type(exc).__name__}: {exc}")
                    record = EventRecord(
                        source_id=event_id,
                        title=title,
                        url=url,
                        keywords=unique_preserving_order(matched_keywords),
                    )
            else:
                record = EventRecord(
                    source_id=event_id,
                    title=title,
                    url=url,
                    keywords=unique_preserving_order(matched_keywords),
                )
            records.append(record)
            fetched_details += 1
            if checkpoint and checkpoint_every > 0 and fetched_details % checkpoint_every == 0:
                checkpoint(records)
        if progress:
            progress(f"[eventernote] detail done records={len(records)} fetched_new={fetched_details}")
        if checkpoint:
            checkpoint(records)
        return records

    def collect_actor_events(
        self,
        actor_urls: Iterable[str],
        max_pages: int | None = 1,
        include_details: bool = True,
        progress: Callable[[str], None] | None = None,
        continue_on_error: bool = True,
        checkpoint: Callable[[list[EventRecord]], None] | None = None,
        checkpoint_every: int = 50,
        existing_records: list[EventRecord] | None = None,
        max_new_details: int | None = None,
    ) -> list[EventRecord]:
        candidates: dict[str, tuple[str, str, list[str]]] = {}
        actor_url_list = list(actor_urls)
        if progress:
            progress(f"[eventernote] actor search start actor_urls={len(actor_url_list)} max_pages={max_pages or 'all'}")
        for actor_url in actor_url_list:
            before = len(candidates)
            if progress:
                progress(f"[eventernote] actor start: {actor_url}")
            for event_id, url, title in self._iter_actor_event_results(
                actor_url,
                max_pages=max_pages,
                progress=progress,
                continue_on_error=continue_on_error,
            ):
                existing = candidates.get(event_id)
                if existing:
                    existing[2].append(actor_url)
                    continue
                candidates[event_id] = (url, title, [actor_url])
            if progress:
                progress(f"[eventernote] actor done: {actor_url} new={len(candidates) - before} total={len(candidates)}")

        records: list[EventRecord] = list(existing_records or [])
        existing_by_id = {record.source_id: record for record in records}
        total = len(candidates)
        if progress:
            progress(
                f"[eventernote] detail start candidates={total} cached={len(existing_by_id)} "
                f"include_details={include_details} max_new_details={max_new_details or 'all'}"
            )
        fetched_details = 0
        for index, (event_id, (url, title, matched_keywords)) in enumerate(candidates.items(), start=1):
            if progress and (index == 1 or index % 10 == 0 or index == total):
                progress(f"[eventernote] detail {index}/{total} event_id={event_id}")
            existing_record = existing_by_id.get(event_id)
            if existing_record:
                existing_record.keywords = unique_preserving_order([*existing_record.keywords, *matched_keywords])
                continue
            if max_new_details is not None and fetched_details >= max_new_details:
                if progress:
                    progress(f"[eventernote] detail limit reached max_new_details={max_new_details}")
                break
            if include_details:
                try:
                    record = self.fetch_event_detail(url)
                    record.keywords = unique_preserving_order(matched_keywords)
                except Exception as exc:
                    if not continue_on_error:
                        raise
                    if progress:
                        progress(f"[eventernote] detail error event_id={event_id} error={type(exc).__name__}: {exc}")
                    record = EventRecord(
                        source_id=event_id,
                        title=title,
                        url=url,
                        keywords=unique_preserving_order(matched_keywords),
                    )
            else:
                record = EventRecord(
                    source_id=event_id,
                    title=title,
                    url=url,
                    keywords=unique_preserving_order(matched_keywords),
                )
            records.append(record)
            fetched_details += 1
            if checkpoint and checkpoint_every > 0 and fetched_details % checkpoint_every == 0:
                checkpoint(records)
        if progress:
            progress(f"[eventernote] detail done records={len(records)} fetched_new={fetched_details}")
        if checkpoint:
            checkpoint(records)
        return records

    def collect_user_events(
        self,
        handle: str,
        max_pages: int | None = 1,
        include_details: bool = True,
        progress: Callable[[str], None] | None = None,
        continue_on_error: bool = True,
        checkpoint: Callable[[list[EventRecord]], None] | None = None,
        checkpoint_every: int = 50,
        existing_records: list[EventRecord] | None = None,
        max_new_details: int | None = None,
    ) -> list[EventRecord]:
        candidates: dict[str, tuple[str, str, list[str]]] = {}
        user_url = self._user_events_url(handle)
        keyword = f"user:{self._handle_from_user_events_url(user_url)}"
        if progress:
            progress(f"[eventernote] user search start handle={handle} max_pages={max_pages or 'all'}")
        for event_id, url, title in self._iter_user_event_results(
            user_url,
            max_pages=max_pages,
            progress=progress,
            continue_on_error=continue_on_error,
        ):
            existing = candidates.get(event_id)
            if existing:
                existing[2].append(keyword)
                continue
            candidates[event_id] = (url, title, [keyword])

        records: list[EventRecord] = list(existing_records or [])
        existing_by_id = {record.source_id: record for record in records}
        total = len(candidates)
        if progress:
            progress(
                f"[eventernote] user detail start candidates={total} cached={len(existing_by_id)} "
                f"include_details={include_details} max_new_details={max_new_details or 'all'}"
            )
        fetched_details = 0
        for index, (event_id, (url, title, matched_keywords)) in enumerate(candidates.items(), start=1):
            if progress and (index == 1 or index % 10 == 0 or index == total):
                progress(f"[eventernote] user detail {index}/{total} event_id={event_id}")
            existing_record = existing_by_id.get(event_id)
            if existing_record:
                existing_record.keywords = unique_preserving_order([*existing_record.keywords, *matched_keywords])
                continue
            if max_new_details is not None and fetched_details >= max_new_details:
                if progress:
                    progress(f"[eventernote] user detail limit reached max_new_details={max_new_details}")
                break
            if include_details:
                try:
                    record = self.fetch_event_detail(url)
                    record.keywords = unique_preserving_order(matched_keywords)
                except Exception as exc:
                    if not continue_on_error:
                        raise
                    if progress:
                        progress(f"[eventernote] user detail error event_id={event_id} error={type(exc).__name__}: {exc}")
                    record = EventRecord(
                        source_id=event_id,
                        title=title,
                        url=url,
                        keywords=unique_preserving_order(matched_keywords),
                    )
            else:
                record = EventRecord(
                    source_id=event_id,
                    title=title,
                    url=url,
                    keywords=unique_preserving_order(matched_keywords),
                )
            records.append(record)
            fetched_details += 1
            if checkpoint and checkpoint_every > 0 and fetched_details % checkpoint_every == 0:
                checkpoint(records)
        if progress:
            progress(f"[eventernote] user detail done records={len(records)} fetched_new={fetched_details}")
        if checkpoint:
            checkpoint(records)
        return records

    def fetch_event_detail(self, url: str, keyword: str | None = None) -> EventRecord:
        html = self.fetcher.get_text(url)
        soup = BeautifulSoup(html, "html.parser")
        text = clean_text(soup.get_text(" ", strip=True))
        source_id = self._event_id_from_url(url)
        title = self._extract_title(soup) or f"eventernote:{source_id}"
        fields = self._extract_detail_fields(soup)
        performers = self._extract_performers(soup)
        related_links = self._extract_related_links(soup)
        date_text = fields.get("開催日時") or ""
        time_text = fields.get("時間") or ""
        attendee_count = first_match(r"このイベントに参加のイベンター\((\d+)人\)", text)

        return EventRecord(
            source_id=source_id,
            title=title,
            url=url,
            event_date=first_match(r"(\d{4}-\d{2}-\d{2})", date_text) or first_match(r"(\d{4}-\d{2}-\d{2})", text),
            open_time=first_match(r"開場\s*([0-2]?\d:[0-5]\d)", time_text),
            start_time=first_match(r"開演\s*([0-2]?\d:[0-5]\d)", time_text),
            end_time=first_match(r"終演\s*([0-2]?\d:[0-5]\d)", time_text),
            venue=fields.get("開催場所"),
            venue_url=self._extract_venue_url(soup),
            performers=performers,
            related_links=related_links,
            keywords=[keyword] if keyword else [],
            attendee_count=int(attendee_count) if attendee_count else None,
            description=fields.get("ライブ概要") or fields.get("イベント概要"),
            raw_text=text,
        )

    def _iter_search_results(
        self,
        keyword: str,
        max_pages: int | None,
        progress: Callable[[str], None] | None = None,
        continue_on_error: bool = True,
    ) -> Iterable[tuple[str, str, str]]:
        next_url = f"{BASE_URL}/events/search?{urlencode({'keyword': keyword})}"
        visited_pages: set[str] = set()
        page_count = 0
        while next_url and next_url not in visited_pages:
            visited_pages.add(next_url)
            page_count += 1
            try:
                html = self.fetcher.get_text(next_url)
            except Exception as exc:
                if not continue_on_error:
                    raise
                if progress:
                    progress(f"[eventernote] keyword={keyword} page={page_count} error={type(exc).__name__}: {exc}")
                break
            soup = BeautifulSoup(html, "html.parser")
            page_results: list[tuple[str, str, str]] = []
            for link in soup.find_all("a", href=True):
                href = str(link["href"])
                match = EVENT_RE.match(href)
                if not match:
                    continue
                title = clean_text(link.get_text(" ", strip=True))
                if not title:
                    heading = link.find_parent(["h3", "h4", "div", "li"])
                    title = clean_text(heading.get_text(" ", strip=True)) if heading else ""
                page_results.append((match.group("id"), absolute_url(BASE_URL, href) or href, title))
            if progress:
                progress(f"[eventernote] keyword={keyword} page={page_count} results={len(page_results)}")
            if not page_results:
                break
            for result in page_results:
                yield result
            if max_pages is not None and page_count >= max_pages:
                break
            next_url = self._find_next_page(soup, expected_path="/events/search")

    def _iter_actor_event_results(
        self,
        actor_url: str,
        max_pages: int | None,
        progress: Callable[[str], None] | None = None,
        continue_on_error: bool = True,
    ) -> Iterable[tuple[str, str, str]]:
        next_url = actor_url
        visited_pages: set[str] = set()
        page_count = 0
        expected_path = unquote(urlparse(actor_url).path)
        while next_url and next_url not in visited_pages:
            visited_pages.add(next_url)
            page_count += 1
            try:
                html = self.fetcher.get_text(next_url)
            except Exception as exc:
                if not continue_on_error:
                    raise
                if progress:
                    progress(f"[eventernote] actor={actor_url} page={page_count} error={type(exc).__name__}: {exc}")
                break
            soup = BeautifulSoup(html, "html.parser")
            page_results: list[tuple[str, str, str]] = []
            for link in soup.find_all("a", href=True):
                href = str(link["href"])
                match = EVENT_RE.match(unquote(urlparse(href).path))
                if not match:
                    continue
                title = clean_text(link.get_text(" ", strip=True))
                if not title:
                    heading = link.find_parent(["h3", "h4", "div", "li"])
                    title = clean_text(heading.get_text(" ", strip=True)) if heading else ""
                page_results.append((match.group("id"), absolute_url(BASE_URL, href) or href, title))
            if progress:
                progress(f"[eventernote] actor={actor_url} page={page_count} results={len(page_results)}")
            if not page_results:
                break
            for result in page_results:
                yield result
            if max_pages is not None and page_count >= max_pages:
                break
            next_url = self._find_next_page(soup, expected_path=expected_path)

    def _iter_user_event_results(
        self,
        user_url: str,
        max_pages: int | None,
        progress: Callable[[str], None] | None = None,
        continue_on_error: bool = True,
    ) -> Iterable[tuple[str, str, str]]:
        next_url = user_url
        visited_pages: set[str] = set()
        page_count = 0
        expected_path = unquote(urlparse(user_url).path)
        while next_url and next_url not in visited_pages:
            visited_pages.add(next_url)
            page_count += 1
            try:
                html = self.fetcher.get_text(next_url)
            except Exception as exc:
                if not continue_on_error:
                    raise
                if progress:
                    progress(f"[eventernote] user={user_url} page={page_count} error={type(exc).__name__}: {exc}")
                break
            soup = BeautifulSoup(html, "html.parser")
            page_results: list[tuple[str, str, str]] = []
            for link in soup.find_all("a", href=True):
                href = str(link["href"])
                match = EVENT_RE.match(unquote(urlparse(href).path))
                if not match:
                    continue
                title = clean_text(link.get_text(" ", strip=True))
                if not title:
                    heading = link.find_parent(["h3", "h4", "div", "li"])
                    title = clean_text(heading.get_text(" ", strip=True)) if heading else ""
                page_results.append((match.group("id"), absolute_url(BASE_URL, href) or href, title))
            page_results = unique_preserving_order_records(page_results)
            if progress:
                progress(f"[eventernote] user={user_url} page={page_count} results={len(page_results)}")
            if not page_results:
                break
            for result in page_results:
                yield result
            if max_pages is not None and page_count >= max_pages:
                break
            next_url = self._find_next_page(soup, expected_path=expected_path)

    def _find_next_page(self, soup: BeautifulSoup, expected_path: str | None = None) -> str | None:
        for link in soup.find_all("a", href=True):
            label = clean_text(link.get_text(" ", strip=True))
            href = str(link["href"])
            parsed = urlparse(href)
            parsed_path = unquote(parsed.path)
            if label in {"次へ", "Next", ">", "»"} and (expected_path is None or parsed_path == expected_path):
                return absolute_url(BASE_URL, href)
        return None

    def _extract_title(self, soup: BeautifulSoup) -> str | None:
        for selector in ("h1", "h2", "title"):
            node = soup.select_one(selector)
            title = clean_text(node.get_text(" ", strip=True)) if node else ""
            if title:
                return title.replace(" Eventernote イベンターノート", "").strip()
        return None

    def _extract_detail_fields(self, soup: BeautifulSoup) -> dict[str, str]:
        fields: dict[str, str] = {}
        for row in soup.find_all("tr"):
            cells = [cell for cell in row.find_all(["th", "td"], recursive=False)]
            if len(cells) < 2:
                continue
            label = clean_text(cells[0].get_text(" ", strip=True))
            value = clean_text(cells[1].get_text(" ", strip=True))
            if label and value:
                fields[label] = value
        return fields

    def _extract_performers(self, soup: BeautifulSoup) -> list[str]:
        for row in soup.find_all("tr"):
            cells = [cell for cell in row.find_all(["th", "td"], recursive=False)]
            if len(cells) < 2 or clean_text(cells[0].get_text(" ", strip=True)) != "出演者":
                continue
            return unique_preserving_order(
                [clean_text(link.get_text(" ", strip=True)) for link in cells[1].select('a[href^="/actors/"]')]
            )
        return []

    def _extract_related_links(self, soup: BeautifulSoup) -> list[SourceLink]:
        links: list[SourceLink] = []
        rows = soup.find_all("tr")
        for row in rows:
            label_cell = row.find(["th", "td"])
            if not label_cell or clean_text(label_cell.get_text(" ", strip=True)) != "関連リンク":
                continue
            for link in row.find_all("a", href=True):
                url = absolute_url(BASE_URL, str(link["href"]))
                if url:
                    links.append(SourceLink(label=clean_text(link.get_text(" ", strip=True)) or None, url=url))
        return links

    def _extract_venue_url(self, soup: BeautifulSoup) -> str | None:
        for row in soup.find_all("tr"):
            cells = [cell for cell in row.find_all(["th", "td"], recursive=False)]
            if len(cells) < 2 or clean_text(cells[0].get_text(" ", strip=True)) != "開催場所":
                continue
            link = cells[1].find("a", href=True)
            if isinstance(link, Tag):
                return absolute_url(BASE_URL, str(link["href"]))
        return None

    def _event_id_from_url(self, url: str) -> str:
        match = re.search(r"/events/(\d+)", url)
        if not match:
            raise ValueError(f"Cannot parse Eventernote event id from {url}")
        return match.group(1)

    def _user_events_url(self, handle: str) -> str:
        if handle.startswith("http://") or handle.startswith("https://"):
            parsed = urlparse(handle)
            if parsed.path.startswith("/users/") and not parsed.path.rstrip("/").endswith("/events"):
                return f"{BASE_URL}{parsed.path.rstrip('/')}/events"
            return handle
        return f"{BASE_URL}/users/{handle.strip('/')}/events"

    def _handle_from_user_events_url(self, url: str) -> str:
        parts = [part for part in urlparse(url).path.split("/") if part]
        if len(parts) >= 2 and parts[0] == "users":
            return parts[1]
        return url

    def _merge_keywords(self, records: list[EventRecord]) -> list[EventRecord]:
        by_id: dict[str, EventRecord] = {}
        for record in records:
            existing = by_id.get(record.source_id)
            if not existing:
                by_id[record.source_id] = record
                continue
            existing.keywords = unique_preserving_order([*existing.keywords, *record.keywords])
        return list(by_id.values())


def unique_preserving_order_records(values: list[tuple[str, str, str]]) -> list[tuple[str, str, str]]:
    seen: set[str] = set()
    result: list[tuple[str, str, str]] = []
    for event_id, url, title in values:
        if event_id in seen:
            continue
        seen.add(event_id)
        result.append((event_id, url, title))
    return result

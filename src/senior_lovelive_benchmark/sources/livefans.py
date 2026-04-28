from __future__ import annotations

import re
from collections.abc import Iterable
from urllib.parse import urlencode

from bs4 import BeautifulSoup, Tag

from senior_lovelive_benchmark.http_client import Fetcher
from senior_lovelive_benchmark.models import EventRecord, SetlistRecord, SongRecord
from senior_lovelive_benchmark.text_utils import (
    absolute_url,
    clean_text,
    first_match,
    normalize_text,
    unique_preserving_order,
)

BASE_URL = "https://www.livefans.jp"
EVENT_RE = re.compile(r"^/events/(?P<id>\d+)")
SONG_RE = re.compile(r"^/songs/(?P<id>\d+)")
GROUP_TERMS = (
    "LoveLive!",
    "ラブライブ",
    "Liella!",
    "Aqours",
    "虹ヶ咲学園スクールアイドル同好会",
    "蓮ノ空女学院スクールアイドルクラブ",
    "μ's",
    "μ’s",
    "Saint Snow",
    "Guilty Kiss",
    "CYaRon!",
    "AZALEA",
)


class LiveFansClient:
    def __init__(self, fetcher: Fetcher | None = None) -> None:
        self.fetcher = fetcher or Fetcher()

    def search_setlists(self, keywords: Iterable[str], max_pages: int | None = 1) -> list[SetlistRecord]:
        records: list[SetlistRecord] = []
        for keyword in keywords:
            for event_id, url in self._iter_search_results(keyword, max_pages=max_pages):
                record = self.fetch_event_setlist(url, event_id=event_id)
                if self._is_lovelive_related(record):
                    records.append(record)
        return self._dedupe(records)

    def collect_for_events(
        self,
        events: Iterable[EventRecord],
        fallback_keywords: Iterable[str],
        max_pages_per_query: int | None = 1,
        max_queries_per_event: int = 3,
    ) -> list[SetlistRecord]:
        records: list[SetlistRecord] = []
        seen_queries: set[str] = set()
        for event in events:
            for query in self._queries_for_event(event, max_queries=max_queries_per_event):
                if query in seen_queries:
                    continue
                seen_queries.add(query)
                records.extend(self.search_setlists([query], max_pages=max_pages_per_query))
        if not records:
            records.extend(self.search_setlists(fallback_keywords, max_pages=max_pages_per_query))
        return self._dedupe(records)

    def fetch_event_setlist(self, url: str, event_id: str | None = None) -> SetlistRecord:
        html = self.fetcher.get_text(url)
        soup = BeautifulSoup(html, "html.parser")
        text = clean_text(soup.get_text(" ", strip=True))
        source_id = event_id or self._event_id_from_url(url)
        title = self._extract_title(soup) or f"livefans:{source_id}"
        venue, venue_url = self._extract_venue(text, soup)
        artist_names = self._extract_artists(text, soup)
        songs = self._extract_songs(soup)
        date_text = self._extract_event_date(text)

        return SetlistRecord(
            source="livefans",
            source_id=source_id,
            title=title,
            url=url,
            event_date=date_text,
            start_time=first_match(r"([0-2]?\d:[0-5]\d)\s*開演", text),
            venue=venue,
            venue_url=venue_url,
            artists=artist_names,
            songs=songs,
            raw_text=text,
        )

    def _iter_search_results(self, keyword: str, max_pages: int | None) -> Iterable[tuple[str, str]]:
        next_url = f"{BASE_URL}/search?{urlencode({'option': 1, 'keyword': keyword, 'setlist': 1, 'sort': 'e1'})}"
        visited_pages: set[str] = set()
        page_count = 0
        while next_url and next_url not in visited_pages:
            visited_pages.add(next_url)
            page_count += 1
            html = self.fetcher.get_text(next_url)
            soup = BeautifulSoup(html, "html.parser")
            yielded = False
            for link in soup.find_all("a", href=True):
                href = str(link["href"])
                match = EVENT_RE.match(href)
                if not match:
                    continue
                yield match.group("id"), absolute_url(BASE_URL, href) or href
                yielded = True
            if not yielded:
                break
            if max_pages is not None and page_count >= max_pages:
                break
            next_url = self._find_next_page(soup)
        return

    def _find_next_page(self, soup: BeautifulSoup) -> str | None:
        for link in soup.find_all("a", href=True):
            label = clean_text(link.get_text(" ", strip=True))
            if "Next" in label or "次" in label:
                return absolute_url(BASE_URL, str(link["href"]))
        return None

    def _queries_for_event(self, event: EventRecord, max_queries: int) -> list[str]:
        queries = [event.title]
        simplified_title = self._simplify_event_title(event.title)
        if simplified_title != event.title:
            queries.append(simplified_title)

        year = event.event_date[:4] if event.event_date else ""
        for performer in event.performers:
            if performer in GROUP_TERMS:
                queries.append(f"{performer} {year}".strip())

        return unique_preserving_order(queries)[:max_queries]

    def _simplify_event_title(self, title: str) -> str:
        simplified = re.sub(r"【[^】]+】", " ", title)
        simplified = re.sub(r"[＜<][^＞>]*Day[.\s]*\d+[^＞>]*[＞>]", " ", simplified, flags=re.IGNORECASE)
        simplified = re.sub(r"\bDay[.\s]*\d+\b", " ", simplified, flags=re.IGNORECASE)
        simplified = re.sub(r"(昼公演|夜公演|有料生配信|ライブビューイング|配信)", " ", simplified)
        for term in GROUP_TERMS:
            simplified = re.sub(rf"\s*{re.escape(term)}\s*$", "", simplified)
        return clean_text(simplified)

    def _extract_title(self, soup: BeautifulSoup) -> str | None:
        for selector in ("h1", "h2", "h3", "title"):
            node = soup.select_one(selector)
            title = clean_text(node.get_text(" ", strip=True)) if node else ""
            if title:
                return title.replace(" | ライブ・セットリスト情報サービス【LiveFans (ライブファンズ) 】", "").strip()
        return None

    def _extract_venue(self, text: str, soup: BeautifulSoup) -> tuple[str | None, str | None]:
        venue = first_match(r"\d{4}/\d{2}/\d{2}\s*\([^)]*\)\s*[0-2]?\d:[0-5]\d\s*開演\s*(＠.+?)(?:\s*\[出演\]|\s*\[ゲスト\]|\s*この公演情報)", text)
        venue_link = self._first_link(soup, "/venues/")
        return (
            venue or (clean_text(venue_link.get_text(" ", strip=True)) if venue_link else None),
            absolute_url(BASE_URL, str(venue_link["href"])) if venue_link else None,
        )

    def _extract_artists(self, text: str, soup: BeautifulSoup) -> list[str]:
        artists: list[str] = []
        performer_block = first_match(r"\[出演\]\s*(.+?)(?:\s*\[ゲスト\]|\s*この公演情報|\s*ポスト|\s*セットリスト)", text)
        if performer_block:
            artists.extend(part.strip() for part in performer_block.split("/") if part.strip())
        guest_block = first_match(r"\[ゲスト\]\s*(.+?)(?:\s*この公演情報|\s*ポスト|\s*セットリスト)", text)
        if guest_block:
            artists.extend(part.strip() for part in guest_block.split("/") if part.strip())
        if artists:
            return unique_preserving_order(artists)
        artists = [clean_text(link.get_text(" ", strip=True)) for link in soup.select('a[href^="/artists/"]')]
        return unique_preserving_order(artists)

    def _extract_songs(self, soup: BeautifulSoup) -> list[SongRecord]:
        songs: list[SongRecord] = []
        for link in soup.find_all("a", href=True):
            href = str(link["href"])
            match = SONG_RE.match(href)
            if not match:
                continue
            song_id = match.group("id")
            title = clean_text(link.get_text(" ", strip=True))
            if not title:
                continue
            songs.append(
                SongRecord(
                    position=len(songs) + 1,
                    title=title,
                    artist=self._artist_after_song_link(link),
                    source_song_id=song_id,
                )
            )
        return songs

    def _extract_event_date(self, text: str) -> str | None:
        dated_start = re.search(r"(\d{4})/(\d{2})/(\d{2})\s*\([^)]*\)\s*[0-2]?\d:[0-5]\d\s*開演", text)
        if dated_start:
            return "-".join(dated_start.groups())
        title_date = re.search(r"\((\d{4})\.(\d{2})\.(\d{2})\)", text)
        if title_date:
            return "-".join(title_date.groups())
        slash_date = first_match(r"(\d{4}/\d{2}/\d{2})", text)
        return slash_date.replace("/", "-") if slash_date else None

    def _is_lovelive_related(self, record: SetlistRecord) -> bool:
        haystack = normalize_text(
            " ".join(
                [
                    record.title,
                    " ".join(record.artists),
                ]
            )
        )
        terms = (
            "lovelive",
            "ラブライブ",
            "liella",
            "aqours",
            "nijigasaki",
            "虹ヶ咲",
            "蓮ノ空",
            "μ s",
            "saint snow",
        )
        return any(term in haystack for term in terms)

    def _artist_after_song_link(self, link: Tag) -> str | None:
        parent_text = clean_text(link.parent.get_text(" ", strip=True)) if link.parent else ""
        escaped_title = re.escape(clean_text(link.get_text(" ", strip=True)))
        artist = first_match(rf"{escaped_title}\s*[（(]([^）)]+)[）)]", parent_text)
        return artist

    def _first_link(self, soup: BeautifulSoup, prefix: str) -> Tag | None:
        link = soup.select_one(f'a[href^="{prefix}"]')
        return link if isinstance(link, Tag) else None

    def _event_id_from_url(self, url: str) -> str:
        match = re.search(r"/events/(\d+)", url)
        if not match:
            raise ValueError(f"Cannot parse LiveFans event id from {url}")
        return match.group(1)

    def _dedupe(self, records: list[SetlistRecord]) -> list[SetlistRecord]:
        by_id: dict[str, SetlistRecord] = {}
        for record in records:
            by_id[record.source_id] = record
        return list(by_id.values())

from __future__ import annotations

from datetime import datetime
from typing import Any

from senior_lovelive_benchmark.http_client import Fetcher
from senior_lovelive_benchmark.models import EventRecord, SetlistRecord, SongRecord
from senior_lovelive_benchmark.text_utils import clean_text, unique_preserving_order

API_URL = "https://api.setlist.fm/rest/1.0/search/setlists"
DEFAULT_ARTISTS = (
    "Liella!",
    "Aqours",
    "μ's",
    "Nijigasaki High School Idol Club",
    "Guilty Kiss",
    "CYaRon!",
    "AZALEA",
    "Saint Snow",
    "Hasunosora Girls' High School Idol Club",
)


class SetlistFmClient:
    def __init__(self, api_key: str, fetcher: Fetcher | None = None) -> None:
        if not api_key:
            raise ValueError("setlist.fm API key is required")
        self.api_key = api_key
        self.fetcher = fetcher or Fetcher()

    def collect_for_events(
        self,
        events: list[EventRecord],
        max_pages_per_query: int = 1,
    ) -> list[SetlistRecord]:
        records: list[SetlistRecord] = []
        for event in events:
            artist_names = self._artist_candidates(event)
            for artist_name in artist_names:
                records.extend(
                    self.search_setlists(
                        artist_name=artist_name,
                        event_date=event.event_date,
                        venue_name=event.venue,
                        max_pages=max_pages_per_query,
                    )
                )
        return self._dedupe(records)

    def search_default_artists(
        self,
        year: int | None = None,
        max_pages_per_artist: int = 1,
    ) -> list[SetlistRecord]:
        records: list[SetlistRecord] = []
        for artist_name in DEFAULT_ARTISTS:
            records.extend(self.search_setlists(artist_name=artist_name, year=year, max_pages=max_pages_per_artist))
        return self._dedupe(records)

    def search_setlists(
        self,
        artist_name: str,
        event_date: str | None = None,
        venue_name: str | None = None,
        year: int | None = None,
        max_pages: int = 1,
    ) -> list[SetlistRecord]:
        params: dict[str, str | int | None] = {"artistName": artist_name}
        if event_date:
            converted_date = self._to_setlistfm_date(event_date)
            if converted_date:
                params["date"] = converted_date
        if venue_name:
            params["venueName"] = venue_name
        if year:
            params["year"] = year

        records: list[SetlistRecord] = []
        for page in range(1, max_pages + 1):
            params["p"] = page
            payload = self.fetcher.get_json(
                API_URL,
                params=params,
                headers={"Accept": "application/json", "x-api-key": self.api_key},
            )
            setlists = payload.get("setlist") or []
            if not setlists:
                break
            records.extend(self._record_from_payload(item) for item in setlists)
            total = int(payload.get("total") or 0)
            items_per_page = int(payload.get("itemsPerPage") or len(setlists) or 1)
            if page * items_per_page >= total:
                break
        return records

    def _record_from_payload(self, item: dict[str, Any]) -> SetlistRecord:
        artist = item.get("artist") or {}
        venue = item.get("venue") or {}
        tour = item.get("tour") or {}
        event_date = self._from_setlistfm_date(item.get("eventDate"))
        songs: list[SongRecord] = []
        position = 1
        for set_item in item.get("sets", {}).get("set", []) or item.get("set", []):
            for song in set_item.get("song", []) or []:
                song_name = clean_text(song.get("name"))
                if not song_name:
                    continue
                cover = song.get("cover") if isinstance(song.get("cover"), dict) else {}
                with_value = song.get("with")
                with_artist = with_value.get("name") if isinstance(with_value, dict) else with_value
                songs.append(
                    SongRecord(
                        position=position,
                        title=song_name,
                        artist=clean_text(with_artist or cover.get("name") or artist.get("name")) or None,
                        raw=song,
                    )
                )
                position += 1

        title_parts = [artist.get("name"), tour.get("name"), venue.get("name"), event_date]
        title = " - ".join(clean_text(part) for part in title_parts if clean_text(part))
        return SetlistRecord(
            source="setlistfm",
            source_id=str(item.get("id")),
            title=title or str(item.get("id")),
            url=str(item.get("url") or ""),
            event_date=event_date,
            venue=clean_text(venue.get("name")) or None,
            venue_url=venue.get("url"),
            artists=[clean_text(artist.get("name"))] if clean_text(artist.get("name")) else [],
            tour=clean_text(tour.get("name")) or None,
            songs=songs,
            raw=item,
        )

    def _artist_candidates(self, event: EventRecord) -> list[str]:
        candidates = [artist for artist in DEFAULT_ARTISTS if artist.casefold() in event.title.casefold()]
        candidates.extend(
            performer
            for performer in event.performers
            if performer in DEFAULT_ARTISTS or any(name.casefold() in performer.casefold() for name in DEFAULT_ARTISTS)
        )
        if not candidates:
            candidates.extend(DEFAULT_ARTISTS)
        return unique_preserving_order(candidates)

    def _to_setlistfm_date(self, event_date: str) -> str | None:
        try:
            return datetime.strptime(event_date, "%Y-%m-%d").strftime("%d-%m-%Y")
        except ValueError:
            return None

    def _from_setlistfm_date(self, event_date: str | None) -> str | None:
        if not event_date:
            return None
        try:
            return datetime.strptime(event_date, "%d-%m-%Y").strftime("%Y-%m-%d")
        except ValueError:
            return None

    def _dedupe(self, records: list[SetlistRecord]) -> list[SetlistRecord]:
        by_id: dict[str, SetlistRecord] = {}
        for record in records:
            by_id[record.source_id] = record
        return list(by_id.values())

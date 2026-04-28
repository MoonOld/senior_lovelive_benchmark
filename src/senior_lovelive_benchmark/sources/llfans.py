from __future__ import annotations

import re
from collections.abc import Callable, Iterable
from dataclasses import dataclass

from rapidfuzz import fuzz

from senior_lovelive_benchmark.http_client import Fetcher
from senior_lovelive_benchmark.models import DiscographySongRecord, EventRecord, SetlistRecord, SongRecord
from senior_lovelive_benchmark.text_utils import clean_text, normalize_text, unique_preserving_order

BASE_URL = "https://ll-fans.jp"
GRAPHQL_URL = "https://api.ll-fans.jp/graphql"
SERIES_GROUP_NAMES = {
    1: "μ’s",
    2: "Aqours",
    3: "虹ヶ咲学園スクールアイドル同好会",
    4: "Liella!",
    5: "スクールアイドルミュージカル",
    6: "蓮ノ空女学院スクールアイドルクラブ",
    7: "幻日のヨハネ",
    8: "いきづらい部！",
}

SERIES_LIST_QUERY = """
query SeriesListContextQuery {
  seriesList {
    id
    name
    color
  }
}
"""

TOUR_LIST_QUERY = """
query TourListPage(
  $filter: TourFilterInput
  $orderBy: [QueryToursOrderByOrderByClause!]
  $page: Int!
  $first: Int!
) {
  tours(filter: $filter, orderBy: $orderBy, first: $first, page: $page) {
    paginatorInfo {
      currentPage
      hasMorePages
      total
    }
    data {
      id
      name
      tourTypeId
      seriesIds
      startsOn
      endsOn
    }
  }
}
"""

DISCOGRAPHY_LIST_QUERY = """
query DiscographyListPage(
  $filter: DiscographyFilterInput
  $orderBy: [QueryDiscographiesOrderByOrderByClause!]
  $page: Int!
  $first: Int!
) {
  discographies(filter: $filter, orderBy: $orderBy, first: $first, page: $page) {
    paginatorInfo {
      currentPage
      hasMorePages
      total
    }
    data {
      id
      name
      description
      releasedAt
      seriesIds
      discographyTypeId
    }
  }
}
"""

DISCOGRAPHY_DETAIL_QUERY = """
query DiscographyDetailPage($id: ID!) {
  discography(id: $id) {
    id
    name
    description
    releasedAt
    seriesIds
    discographyTypeId
    discographyVersions {
      id
      name
      discs {
        id
        discNumber
        discTracks {
          id
          contentType
          content {
            __typename
            ... on SongVersion {
              id
              name
              songId
              song {
                id
                name
                seriesIds
              }
            }
          }
        }
      }
    }
  }
}
"""

TOUR_DETAIL_QUERY = """
query EventDetailPage($id: ID!) {
  tour(id: $id) {
    id
    name
    seriesIds
    startsOn
    endsOn
    url
    note
    tourType {
      name
    }
    concerts(orderBy: [{column: STARTS_ON, order: ASC}]) {
      id
      name
      startsOn
      endsOn
      note
      venue {
        id
        name
      }
      performances(orderBy: [{column: DATE, order: ASC}, {column: START_TIME, order: ASC}]) {
        id
        name
        date
        openTime
        startTime
        note
        setlists(orderBy: [{column: ORDER, order: ASC}, {column: ID, order: ASC}]) {
          id
          indexPrefix
          indexNumber
          content {
            __typename
            ... on Song {
              id
              name
              seriesIds
            }
            ... on CollaborationSong {
              name
            }
          }
          contentTypeOther
          note
          premiere
        }
      }
    }
  }
}
"""


@dataclass
class LLFansTourSummary:
    source_id: str
    title: str
    starts_on: str | None
    ends_on: str | None


@dataclass
class LLFansDiscographySummary:
    source_id: str
    title: str
    description: str | None
    released_at: str | None
    series_ids: list[int]
    discography_type_id: str | None


class LLFansClient:
    def __init__(self, fetcher: Fetcher | None = None) -> None:
        self.fetcher = fetcher or Fetcher()

    def collect_for_events(
        self,
        events: Iterable[EventRecord],
        max_candidate_tours: int = 5,
        first: int = 1000,
        progress: Callable[[str], None] | None = None,
    ) -> list[SetlistRecord]:
        tours = self.list_tours(first=first)
        records: list[SetlistRecord] = []
        seen_tour_ids: set[str] = set()
        event_list = list(events)
        if progress:
            progress(f"[llfans] loaded tours={len(tours)} events={len(event_list)}")
        for index, event in enumerate(event_list, start=1):
            candidate_tours = self._candidate_tours(event, tours, max_candidate_tours)
            if progress:
                progress(f"[llfans] event {index}/{len(event_list)} event_id={event.source_id} candidate_tours={len(candidate_tours)}")
            for tour in candidate_tours:
                if tour.source_id in seen_tour_ids:
                    continue
                seen_tour_ids.add(tour.source_id)
                tour_records = self.fetch_tour_setlists(tour.source_id)
                records.extend(tour_records)
                if progress:
                    progress(f"[llfans] fetched tour_id={tour.source_id} setlists={len(tour_records)} total_setlists={len(records)}")
        return self._dedupe(records)

    def collect_discography_songs(
        self,
        progress: Callable[[str], None] | None = None,
    ) -> list[DiscographySongRecord]:
        series_names_by_id = self.list_series()
        discographies = self.list_discographies(progress=progress)
        records: list[DiscographySongRecord] = []
        for index, discography in enumerate(discographies, start=1):
            discography_records = self.fetch_discography_songs(discography.source_id, series_names_by_id)
            records.extend(discography_records)
            if progress and (index == 1 or index % 25 == 0 or index == len(discographies)):
                progress(
                    f"[llfans] discography {index}/{len(discographies)} "
                    f"discography_id={discography.source_id} songs={len(discography_records)} total={len(records)}"
                )
        return records

    def list_series(self) -> dict[int, str]:
        payload = self._graphql(
            operation_name="SeriesListContextQuery",
            query=SERIES_LIST_QUERY,
            variables={},
        )
        return {int(item["id"]): clean_text(item["name"]) for item in payload["data"]["seriesList"]}

    def list_discographies(
        self,
        first: int = 1000,
        max_pages: int | None = None,
        progress: Callable[[str], None] | None = None,
    ) -> list[LLFansDiscographySummary]:
        records: list[LLFansDiscographySummary] = []
        page = 1
        while True:
            payload = self._graphql(
                operation_name="DiscographyListPage",
                query=DISCOGRAPHY_LIST_QUERY,
                variables={
                    "filter": None,
                    "orderBy": [{"column": "RELEASED_AT", "order": "ASC"}, {"column": "ID", "order": "ASC"}],
                    "page": page,
                    "first": first,
                },
            )
            discographies = payload["data"]["discographies"]
            for item in discographies["data"]:
                records.append(
                    LLFansDiscographySummary(
                        source_id=str(item["id"]),
                        title=clean_text(item["name"]),
                        description=clean_text(item.get("description")) or None,
                        released_at=item.get("releasedAt"),
                        series_ids=[int(series_id) for series_id in item.get("seriesIds") or []],
                        discography_type_id=str(item["discographyTypeId"]) if item.get("discographyTypeId") else None,
                    )
                )
            page_info = discographies["paginatorInfo"]
            if progress:
                progress(
                    f"[llfans] discography page={page_info['currentPage']} "
                    f"items={len(discographies['data'])} total_loaded={len(records)} total={page_info['total']}"
                )
            if not page_info["hasMorePages"]:
                break
            page += 1
            if max_pages is not None and page > max_pages:
                break
        return records

    def fetch_discography_songs(
        self,
        discography_id: str,
        series_names_by_id: dict[int, str] | None = None,
    ) -> list[DiscographySongRecord]:
        payload = self._graphql(
            operation_name="DiscographyDetailPage",
            query=DISCOGRAPHY_DETAIL_QUERY,
            variables={"id": discography_id},
        )
        discography = payload["data"]["discography"]
        if not discography:
            return []
        series_names_by_id = series_names_by_id or self.list_series()
        records: list[DiscographySongRecord] = []
        for version in discography.get("discographyVersions") or []:
            for disc in version.get("discs") or []:
                for track in disc.get("discTracks") or []:
                    content = track.get("content") or {}
                    if content.get("__typename") != "SongVersion":
                        continue
                    song = content.get("song") or {}
                    song_id = song.get("id")
                    title = clean_text(song.get("name"))
                    if not song_id or not title:
                        continue
                    series_ids = [int(series_id) for series_id in song.get("seriesIds") or []]
                    records.append(
                        DiscographySongRecord(
                            source_id=str(track["id"]),
                            source_song_id=str(song_id),
                            title=title,
                            url=f"{BASE_URL}/data/song/{song_id}",
                            series_ids=series_ids,
                            series_names=[series_names_by_id.get(series_id, str(series_id)) for series_id in series_ids],
                            group_names=[
                                SERIES_GROUP_NAMES.get(series_id, series_names_by_id.get(series_id, str(series_id)))
                                for series_id in series_ids
                            ],
                            discography_id=str(discography["id"]),
                            discography_title=clean_text(discography.get("name")),
                            discography_description=clean_text(discography.get("description")) or None,
                            released_at=discography.get("releasedAt"),
                            discography_type_id=(
                                str(discography["discographyTypeId"]) if discography.get("discographyTypeId") else None
                            ),
                            version_id=str(version["id"]) if version.get("id") else None,
                            version_name=clean_text(version.get("name")) or None,
                            disc_id=str(disc["id"]) if disc.get("id") else None,
                            disc_number=disc.get("discNumber"),
                            disc_track_id=str(track["id"]),
                            song_version_id=str(content["id"]) if content.get("id") else None,
                            song_version_name=clean_text(content.get("name")) or None,
                            raw={
                                "content_type": track.get("contentType"),
                                "content_typename": content.get("__typename"),
                            },
                        )
                    )
        return records

    def list_tours(
        self,
        first: int = 1000,
        max_pages: int | None = None,
        progress: Callable[[str], None] | None = None,
    ) -> list[LLFansTourSummary]:
        records: list[LLFansTourSummary] = []
        page = 1
        while True:
            payload = self._graphql(
                operation_name="TourListPage",
                query=TOUR_LIST_QUERY,
                variables={
                    "filter": None,
                    "orderBy": [{"column": "STARTS_ON", "order": "DESC"}, {"column": "ID", "order": "DESC"}],
                    "page": page,
                    "first": first,
                },
            )
            tours = payload["data"]["tours"]
            for item in tours["data"]:
                records.append(
                    LLFansTourSummary(
                        source_id=str(item["id"]),
                        title=clean_text(item["name"]),
                        starts_on=item.get("startsOn"),
                        ends_on=item.get("endsOn"),
                    )
                )
            page_info = tours["paginatorInfo"]
            if progress:
                progress(
                    f"[llfans] tour page={page_info['currentPage']} "
                    f"items={len(tours['data'])} total_loaded={len(records)} total={page_info['total']}"
                )
            if not page_info["hasMorePages"]:
                break
            page += 1
            if max_pages is not None and page > max_pages:
                break
        return records

    def fetch_tour_setlists(self, tour_id: str) -> list[SetlistRecord]:
        payload = self._graphql(
            operation_name="EventDetailPage",
            query=TOUR_DETAIL_QUERY,
            variables={"id": tour_id},
        )
        tour = payload["data"]["tour"]
        if not tour:
            return []
        records: list[SetlistRecord] = []
        for concert in tour.get("concerts") or []:
            venue = concert.get("venue") or {}
            for performance in concert.get("performances") or []:
                songs = self._extract_songs(performance.get("setlists") or [])
                if not songs:
                    continue
                performance_id = str(performance["id"])
                title = self._record_title(tour, concert, performance)
                records.append(
                    SetlistRecord(
                        source="llfans",
                        source_id=performance_id,
                        title=title,
                        url=f"{BASE_URL}/data/event/{tour_id}",
                        event_date=performance.get("date") or concert.get("startsOn"),
                        start_time=self._time_hhmm(performance.get("startTime")),
                        venue=clean_text(venue.get("name")),
                        venue_url=f"{BASE_URL}/data/venue/{venue['id']}" if venue.get("id") else None,
                        artists=self._extract_artists(tour.get("note")),
                        tour=clean_text(tour.get("name")),
                        songs=songs,
                        raw={
                            "tour_id": str(tour["id"]),
                            "concert_id": str(concert["id"]),
                            "performance_id": performance_id,
                            "concert_name": concert.get("name"),
                            "performance_name": performance.get("name"),
                        },
                    )
                )
        return records

    def _graphql(self, operation_name: str, query: str, variables: dict) -> dict:
        payload = self.fetcher.post_json(
            GRAPHQL_URL,
            json={"operationName": operation_name, "query": query, "variables": variables},
            headers={"Content-Type": "application/json"},
        )
        if payload.get("errors"):
            raise RuntimeError(f"LL-Fans GraphQL error: {payload['errors']}")
        return payload

    def _candidate_tours(
        self,
        event: EventRecord,
        tours: list[LLFansTourSummary],
        max_candidate_tours: int,
    ) -> list[LLFansTourSummary]:
        scored: list[tuple[float, LLFansTourSummary]] = []
        for tour in tours:
            if event.event_date and not self._date_in_range(event.event_date, tour.starts_on, tour.ends_on):
                continue
            score = fuzz.token_set_ratio(normalize_text(event.title), normalize_text(tour.title))
            if score >= 35:
                scored.append((score, tour))
        scored.sort(key=lambda item: item[0], reverse=True)
        return [tour for _, tour in scored[:max_candidate_tours]]

    def _date_in_range(self, event_date: str, starts_on: str | None, ends_on: str | None) -> bool:
        if starts_on and event_date < starts_on:
            return False
        if ends_on and event_date > ends_on:
            return False
        return True

    def _record_title(self, tour: dict, concert: dict, performance: dict) -> str:
        parts = [tour.get("name"), concert.get("name"), performance.get("name")]
        return " ".join(clean_text(part) for part in parts if clean_text(part))

    def _extract_songs(self, setlists: list[dict]) -> list[SongRecord]:
        songs: list[SongRecord] = []
        for item in setlists:
            content = item.get("content")
            if not content:
                continue
            title = clean_text(content.get("name"))
            if not title:
                continue
            songs.append(
                SongRecord(
                    position=len(songs) + 1,
                    title=title,
                    source_song_id=str(content["id"]) if content.get("id") else None,
                    raw={
                        "setlist_id": str(item["id"]),
                        "note": item.get("note"),
                        "premiere": item.get("premiere"),
                    },
                )
            )
        return songs

    def _extract_artists(self, note: str | None) -> list[str]:
        if not note:
            return []
        block = note.split("【", 1)[0] if "【" in note else note
        lines = [clean_text(line) for line in note.splitlines()]
        candidates = [line for line in lines if line and "役" not in line and "出演" not in line and "敬称略" not in line]
        return unique_preserving_order(candidates[:5] or [block])

    def _time_hhmm(self, value: str | None) -> str | None:
        if not value:
            return None
        match = re.match(r"([0-2]\d:[0-5]\d)", value)
        return match.group(1) if match else value

    def _dedupe(self, records: list[SetlistRecord]) -> list[SetlistRecord]:
        by_id: dict[str, SetlistRecord] = {}
        for record in records:
            by_id[record.source_id] = record
        return list(by_id.values())

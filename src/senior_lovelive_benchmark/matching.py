from __future__ import annotations

from rapidfuzz import fuzz

from senior_lovelive_benchmark.models import EventRecord, MatchRecord, SetlistRecord
from senior_lovelive_benchmark.text_utils import normalize_text

NON_SETLIST_SUBEVENT_MARKERS = (
    "お見送り会",
    "お渡し",
    "特典会",
    "サイン会",
    "撮影会",
    "舞台挨拶",
    "応援上映",
    "公開録音",
    "番組観覧",
    "生放送観覧",
    "Special Goodbye event",
    "Goodbye Special",
    "Screening",
    "Meet and Greet",
    "Q&A",
    "Panel",
)


def match_events_to_setlists(
    events: list[EventRecord],
    setlists: list[SetlistRecord],
    threshold: float = 65.0,
    top_n: int = 3,
) -> list[MatchRecord]:
    matches: list[MatchRecord] = []
    for event in events:
        scored = sorted(
            (_score_pair(event, setlist) for setlist in setlists),
            key=lambda item: item[0],
            reverse=True,
        )
        for score, reasons, setlist in scored[:top_n]:
            if score < threshold:
                continue
            matches.append(
                MatchRecord(
                    event_source_id=event.source_id,
                    setlist_source=setlist.source,
                    setlist_source_id=setlist.source_id,
                    match_score=round(score, 2),
                    match_reason=reasons,
                    event_url=event.url,
                    setlist_url=setlist.url,
                    event_title=event.title,
                    setlist_title=setlist.title,
                    event_date=event.event_date,
                    setlist_date=setlist.event_date,
                    event_venue=event.venue,
                    setlist_venue=setlist.venue,
                )
            )
    return matches


def _score_pair(event: EventRecord, setlist: SetlistRecord) -> tuple[float, list[str], SetlistRecord]:
    reasons: list[str] = []
    score = 0.0

    if _has_non_setlist_subevent_marker(event.title) and not _has_non_setlist_subevent_marker(setlist.title):
        return 0.0, ["non_setlist_subevent"], setlist

    if event.event_date and setlist.event_date:
        if event.event_date == setlist.event_date:
            score += 35
            reasons.append("date_exact")
        else:
            return 0.0, ["date_mismatch"], setlist
    else:
        score -= 15
        reasons.append("date_missing")

    if event.start_time and setlist.start_time:
        if event.start_time == setlist.start_time:
            score += 10
            reasons.append("start_time_exact")
        else:
            diff_minutes = _time_diff_minutes(event.start_time, setlist.start_time)
            if diff_minutes is None:
                score -= 15
                reasons.append("start_time_unparseable_mismatch")
            elif diff_minutes <= 15:
                score += 5
                reasons.append("start_time_near")
            elif diff_minutes <= 45:
                reasons.append("start_time_close")
            elif diff_minutes <= 90:
                score -= 10
                reasons.append("start_time_mismatch")
            else:
                score -= 25
                reasons.append("start_time_far_mismatch")

    title_score = fuzz.token_set_ratio(normalize_text(event.title), normalize_text(setlist.title))
    score += title_score * 0.25
    if title_score >= 70:
        reasons.append("title_similar")

    if event.venue and setlist.venue:
        venue_score = fuzz.token_set_ratio(normalize_text(event.venue), normalize_text(setlist.venue))
        score += venue_score * 0.2
        if venue_score >= 70:
            reasons.append("venue_similar")

    performer_score = _performer_overlap_score(event.performers, setlist.artists)
    score += performer_score * 0.2
    if performer_score >= 70:
        reasons.append("artist_overlap")

    if setlist.songs:
        score += 5
        reasons.append("has_songs")

    return max(0.0, min(100.0, score)), reasons, setlist


def _performer_overlap_score(event_performers: list[str], setlist_artists: list[str]) -> float:
    if not event_performers or not setlist_artists:
        return 0.0
    best = 0.0
    normalized_performers = [normalize_text(value) for value in event_performers]
    for artist in setlist_artists:
        normalized_artist = normalize_text(artist)
        best = max(best, *(fuzz.token_set_ratio(normalized_artist, performer) for performer in normalized_performers))
    return best


def _time_diff_minutes(left: str, right: str) -> int | None:
    left_parts = left.split(":", 1)
    right_parts = right.split(":", 1)
    if len(left_parts) != 2 or len(right_parts) != 2:
        return None
    try:
        left_minutes = int(left_parts[0]) * 60 + int(left_parts[1])
        right_minutes = int(right_parts[0]) * 60 + int(right_parts[1])
    except ValueError:
        return None
    return abs(left_minutes - right_minutes)


def _has_non_setlist_subevent_marker(title: str) -> bool:
    normalized = normalize_text(title)
    return any(normalize_text(marker) in normalized for marker in NON_SETLIST_SUBEVENT_MARKERS)

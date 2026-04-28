from __future__ import annotations

from pathlib import Path

from senior_lovelive_benchmark.models import EventRecord, MatchRecord, SetlistRecord
from senior_lovelive_benchmark.storage import ensure_parent


def export_matches_markdown(
    path: Path,
    events: list[EventRecord],
    setlists: list[SetlistRecord],
    matches: list[MatchRecord],
) -> int:
    ensure_parent(path)
    events_by_id = {event.source_id: event for event in events}
    setlists_by_key = {(setlist.source, setlist.source_id): setlist for setlist in setlists}
    matches_by_event: dict[str, list[MatchRecord]] = {}
    for match in matches:
        matches_by_event.setdefault(match.event_source_id, []).append(match)

    lines: list[str] = [
        "# LoveLive Event Setlist Matches",
        "",
        f"- Eventernote events: {len(events)}",
        f"- Setlists: {len(setlists)}",
        f"- Matches: {len(matches)}",
        "",
    ]

    for event in sorted(events, key=lambda item: (item.event_date or "9999-99-99", item.title)):
        event_matches = sorted(matches_by_event.get(event.source_id, []), key=lambda item: item.match_score, reverse=True)
        lines.extend(
            [
                f"## {event.event_date or 'Unknown Date'} - {event.title}",
                "",
                f"- Eventernote: {event.url}",
                f"- Venue: {event.venue or 'Unknown'}",
                f"- Performers: {', '.join(event.performers[:12]) if event.performers else 'Unknown'}",
            ]
        )
        if not event_matches:
            lines.extend(["- Matched setlists: none", ""])
            continue

        lines.append("- Matched setlists:")
        for match in event_matches:
            setlist = setlists_by_key.get((match.setlist_source, match.setlist_source_id))
            song_preview = ""
            if setlist and setlist.songs:
                titles = [song.title for song in setlist.songs[:5]]
                song_preview = f" Songs: {', '.join(titles)}"
                if len(setlist.songs) > 5:
                    song_preview += f" ... ({len(setlist.songs)} total)"
            lines.append(
                f"  - {match.match_score:.1f} [{match.setlist_source}] {match.setlist_title} "
                f"({match.setlist_url}) Reasons: {', '.join(match.match_reason)}.{song_preview}"
            )
        lines.append("")

    path.write_text("\n".join(lines), encoding="utf-8")
    return len(matches)

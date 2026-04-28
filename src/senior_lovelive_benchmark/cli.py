from __future__ import annotations

import json
import os
from collections import defaultdict
from pathlib import Path
from typing import Annotated, List, Optional

import typer
from rapidfuzz import fuzz

from senior_lovelive_benchmark.export import export_matches_markdown
from senior_lovelive_benchmark.matching import match_events_to_setlists
from senior_lovelive_benchmark.models import DiscographySongRecord, EventRecord, MatchRecord, SetlistRecord
from senior_lovelive_benchmark.sources.eventernote import (
    DEFAULT_KEYWORDS,
    DEFAULT_MAIN_GROUP_ACTOR_URLS,
    MAIN_GROUP_ACTOR_URLS,
    EventernoteClient,
)
from senior_lovelive_benchmark.sources.llfans import LLFansClient
from senior_lovelive_benchmark.sources.livefans import LiveFansClient
from senior_lovelive_benchmark.sources.setlistfm import SetlistFmClient
from senior_lovelive_benchmark.storage import merge_jsonl, read_jsonl, write_jsonl
from senior_lovelive_benchmark.text_utils import normalize_text

app = typer.Typer(no_args_is_help=True)

DEFAULT_EVENT_PATH = Path("data/raw/eventernote_events.jsonl")
DEFAULT_LLFANS_PATH = Path("data/raw/llfans_setlists.jsonl")
DEFAULT_SETLISTFM_PATH = Path("data/raw/setlistfm_setlists.jsonl")
DEFAULT_LIVEFANS_PATH = Path("data/raw/livefans_setlists.jsonl")
DEFAULT_MATCH_PATH = Path("data/processed/event_setlist_matches.jsonl")
DEFAULT_EXPORT_PATH = Path("data/exports/lovelive_event_setlists.md")
DEFAULT_DISCOGRAPHY_SONGS_PATH = Path("data/raw/llfans_discography_songs.jsonl")
DEFAULT_GROUP_SONG_INDEX_PATH = Path("data/processed/group_song_index.json")
DEFAULT_SONG_COVERAGE_JSON_PATH = Path("data/processed/song_coverage_analysis.json")
DEFAULT_SONG_COVERAGE_MARKDOWN_PATH = Path("data/exports/song_coverage_analysis.md")
DEFAULT_USER_EVENT_PATH = Path("data/raw/eventernote_user_events.jsonl")
DEFAULT_USER_LOVELIVE_MATCH_PATH = Path("data/processed/user_lovelive_events.json")
DEFAULT_USER_LOVELIVE_MARKDOWN_PATH = Path("data/exports/user_lovelive_events.md")
DEFAULT_USER_LOVELIVE_EVENT_IDS_PATH = Path("data/processed/user_lovelive_event_ids.txt")
DEFAULT_HANDLE_SONG_COVERAGE_JSON_PATH = Path("data/processed/handle_song_coverage_analysis.json")
DEFAULT_HANDLE_SONG_COVERAGE_MARKDOWN_PATH = Path("data/exports/handle_song_coverage_analysis.md")


def _ensure_existing_file(path: Path, label: str) -> None:
    if not path.exists():
        raise typer.BadParameter(f"{label} does not exist: {path}")


def _ensure_non_negative(value: float | int, name: str) -> None:
    if value < 0:
        raise typer.BadParameter(f"{name} must be >= 0")


def _ensure_positive(value: float | int, name: str) -> None:
    if value <= 0:
        raise typer.BadParameter(f"{name} must be > 0")


@app.command("crawl-eventernote")
def crawl_eventernote(
    output: Annotated[Path, typer.Option("--output", "-o")] = DEFAULT_EVENT_PATH,
    keyword: Annotated[Optional[List[str]], typer.Option("--keyword", "-k")] = None,
    actor_url: Annotated[Optional[List[str]], typer.Option("--actor-url", help="Eventernote actor events URL, repeatable.")] = None,
    main_group_actors: Annotated[bool, typer.Option("--main-group-actors", help="Use built-in LoveLive main group actor event URLs.")] = False,
    max_pages: Annotated[int, typer.Option("--max-pages", help="Use 0 to crawl until no next page is found.")] = 1,
    details: Annotated[bool, typer.Option("--details/--no-details")] = True,
    max_details: Annotated[int, typer.Option("--max-details", help="Use 0 for no limit. Useful for resumable batches.")] = 0,
    checkpoint_every: Annotated[int, typer.Option("--checkpoint-every", help="Write partial output every N newly fetched details. Use 0 to disable.")] = 50,
    delay: Annotated[float, typer.Option("--delay")] = 1.0,
    verbose: Annotated[bool, typer.Option("--verbose/--quiet")] = True,
    continue_on_error: Annotated[bool, typer.Option("--continue-on-error/--fail-fast")] = True,
    overwrite: Annotated[bool, typer.Option("--overwrite/--merge")] = False,
) -> None:
    _ensure_non_negative(max_pages, "--max-pages")
    _ensure_non_negative(max_details, "--max-details")
    _ensure_non_negative(checkpoint_every, "--checkpoint-every")
    _ensure_non_negative(delay, "--delay")
    keywords = keyword or list(DEFAULT_KEYWORDS)
    client = EventernoteClient()
    client.fetcher.delay_seconds = delay
    existing_records = [] if overwrite or not output.exists() else read_jsonl(output, EventRecord)

    def checkpoint(records: list[EventRecord]) -> None:
        if checkpoint_every <= 0:
            return
        count = write_jsonl(output, records) if overwrite else merge_jsonl(output, records, key=lambda item: item.source_id, model=EventRecord)
        if verbose:
            typer.echo(f"[eventernote] checkpoint wrote={count} output={output}")

    actor_urls = actor_url or (list(DEFAULT_MAIN_GROUP_ACTOR_URLS) if main_group_actors else None)
    if actor_urls:
        records = client.collect_actor_events(
            actor_urls=actor_urls,
            max_pages=None if max_pages == 0 else max_pages,
            include_details=details,
            progress=typer.echo if verbose else None,
            continue_on_error=continue_on_error,
            checkpoint=checkpoint if details else None,
            checkpoint_every=checkpoint_every,
            existing_records=existing_records,
            max_new_details=None if max_details == 0 else max_details,
        )
    else:
        records = client.search_events(
            keywords=keywords,
            max_pages=None if max_pages == 0 else max_pages,
            include_details=details,
            progress=typer.echo if verbose else None,
            continue_on_error=continue_on_error,
            checkpoint=checkpoint if details else None,
            checkpoint_every=checkpoint_every,
            existing_records=existing_records,
            max_new_details=None if max_details == 0 else max_details,
        )
    if overwrite:
        count = write_jsonl(output, records)
    else:
        count = merge_jsonl(output, records, key=lambda item: item.source_id, model=EventRecord)
    typer.echo(f"Wrote {count} Eventernote events to {output}")


@app.command("crawl-user-events")
def crawl_user_events(
    handle: Annotated[str, typer.Argument(help="Eventernote user handle, or a full /users/{handle}/events URL.")],
    output: Annotated[Path, typer.Option("--output", "-o")] = DEFAULT_USER_EVENT_PATH,
    max_pages: Annotated[int, typer.Option("--max-pages", help="Use 0 to crawl until no next page is found.")] = 0,
    details: Annotated[bool, typer.Option("--details/--no-details")] = True,
    max_details: Annotated[int, typer.Option("--max-details", help="Use 0 for no limit. Useful for resumable batches.")] = 0,
    checkpoint_every: Annotated[int, typer.Option("--checkpoint-every", help="Write partial output every N newly fetched details. Use 0 to disable.")] = 50,
    delay: Annotated[float, typer.Option("--delay")] = 1.0,
    verbose: Annotated[bool, typer.Option("--verbose/--quiet")] = True,
    continue_on_error: Annotated[bool, typer.Option("--continue-on-error/--fail-fast")] = True,
    overwrite: Annotated[bool, typer.Option("--overwrite/--merge")] = False,
) -> None:
    if not handle.strip():
        raise typer.BadParameter("handle must not be empty")
    _ensure_non_negative(max_pages, "--max-pages")
    _ensure_non_negative(max_details, "--max-details")
    _ensure_non_negative(checkpoint_every, "--checkpoint-every")
    _ensure_non_negative(delay, "--delay")
    client = EventernoteClient()
    client.fetcher.delay_seconds = delay
    existing_records = [] if overwrite or not output.exists() else read_jsonl(output, EventRecord)

    def checkpoint(records: list[EventRecord]) -> None:
        if checkpoint_every <= 0:
            return
        count = write_jsonl(output, records) if overwrite else merge_jsonl(output, records, key=lambda item: item.source_id, model=EventRecord)
        if verbose:
            typer.echo(f"[eventernote] user checkpoint wrote={count} output={output}")

    records = client.collect_user_events(
        handle=handle,
        max_pages=None if max_pages == 0 else max_pages,
        include_details=details,
        progress=typer.echo if verbose else None,
        continue_on_error=continue_on_error,
        checkpoint=checkpoint if details else None,
        checkpoint_every=checkpoint_every,
        existing_records=existing_records,
        max_new_details=None if max_details == 0 else max_details,
    )
    if overwrite:
        count = write_jsonl(output, records)
    else:
        count = merge_jsonl(output, records, key=lambda item: item.source_id, model=EventRecord)
    typer.echo(f"Wrote {count} Eventernote user events to {output}")


@app.command("match-user-lovelive-events")
def match_user_lovelive_events(
    user_events_path: Annotated[Path, typer.Option("--user-events")] = DEFAULT_USER_EVENT_PATH,
    library_events_path: Annotated[Path, typer.Option("--library-events")] = Path("data/raw/eventernote_main_groups_events.jsonl"),
    output: Annotated[Path, typer.Option("--output", "-o")] = DEFAULT_USER_LOVELIVE_MATCH_PATH,
    markdown_output: Annotated[Path, typer.Option("--markdown-output")] = DEFAULT_USER_LOVELIVE_MARKDOWN_PATH,
    event_ids_output: Annotated[Path, typer.Option("--event-ids-output")] = DEFAULT_USER_LOVELIVE_EVENT_IDS_PATH,
) -> None:
    _ensure_existing_file(user_events_path, "--user-events")
    _ensure_existing_file(library_events_path, "--library-events")
    user_events = read_jsonl(user_events_path, EventRecord)
    library_events = read_jsonl(library_events_path, EventRecord)
    analysis = _build_user_lovelive_event_matches(user_events, library_events)
    _write_user_lovelive_event_match_outputs(analysis, output, markdown_output, event_ids_output)
    typer.echo(f"Wrote user LoveLive matches to {output}")
    typer.echo(f"Wrote user LoveLive markdown to {markdown_output}")
    typer.echo(f"Wrote matched Eventernote event ids to {event_ids_output}")


@app.command("analyze-handle-coverage")
def analyze_handle_coverage(
    handle: Annotated[str, typer.Argument(help="Eventernote user handle, or a full /users/{handle}/events URL.")],
    user_events_output: Annotated[Path, typer.Option("--user-events-output")] = DEFAULT_USER_EVENT_PATH,
    user_matches_output: Annotated[Path, typer.Option("--user-matches-output")] = DEFAULT_USER_LOVELIVE_MATCH_PATH,
    user_matches_markdown_output: Annotated[Path, typer.Option("--user-matches-markdown-output")] = DEFAULT_USER_LOVELIVE_MARKDOWN_PATH,
    event_ids_output: Annotated[Path, typer.Option("--event-ids-output")] = DEFAULT_USER_LOVELIVE_EVENT_IDS_PATH,
    library_events_path: Annotated[Path, typer.Option("--library-events")] = Path("data/raw/eventernote_main_groups_events.jsonl"),
    matches_path: Annotated[Path, typer.Option("--matches")] = Path("data/processed/main_groups_event_setlist_matches.jsonl"),
    llfans_path: Annotated[Path, typer.Option("--llfans")] = Path("data/raw/llfans_all_setlists.jsonl"),
    group_index_path: Annotated[Path, typer.Option("--group-index")] = DEFAULT_GROUP_SONG_INDEX_PATH,
    output: Annotated[Path, typer.Option("--output", "-o")] = DEFAULT_HANDLE_SONG_COVERAGE_JSON_PATH,
    markdown_output: Annotated[Path, typer.Option("--markdown-output")] = DEFAULT_HANDLE_SONG_COVERAGE_MARKDOWN_PATH,
    max_pages: Annotated[int, typer.Option("--max-pages", help="Use 0 to crawl until no next page is found.")] = 0,
    details: Annotated[bool, typer.Option("--details/--no-details")] = False,
    max_details: Annotated[int, typer.Option("--max-details", help="Use 0 for no limit. Only applies with --details.")] = 0,
    checkpoint_every: Annotated[int, typer.Option("--checkpoint-every", help="Write partial user event output every N newly fetched details. Use 0 to disable.")] = 50,
    delay: Annotated[float, typer.Option("--delay")] = 1.0,
    verbose: Annotated[bool, typer.Option("--verbose/--quiet")] = True,
    continue_on_error: Annotated[bool, typer.Option("--continue-on-error/--fail-fast")] = True,
) -> None:
    if not handle.strip():
        raise typer.BadParameter("handle must not be empty")
    _ensure_non_negative(max_pages, "--max-pages")
    _ensure_non_negative(max_details, "--max-details")
    _ensure_non_negative(checkpoint_every, "--checkpoint-every")
    _ensure_non_negative(delay, "--delay")
    _ensure_existing_file(library_events_path, "--library-events")
    _ensure_existing_file(matches_path, "--matches")
    _ensure_existing_file(llfans_path, "--llfans")
    _ensure_existing_file(group_index_path, "--group-index")

    client = EventernoteClient()
    client.fetcher.delay_seconds = delay

    def checkpoint(records: list[EventRecord]) -> None:
        if checkpoint_every <= 0:
            return
        count = write_jsonl(user_events_output, records)
        if verbose:
            typer.echo(f"[eventernote] handle coverage checkpoint wrote={count} output={user_events_output}")

    user_events = client.collect_user_events(
        handle=handle,
        max_pages=None if max_pages == 0 else max_pages,
        include_details=details,
        progress=typer.echo if verbose else None,
        continue_on_error=continue_on_error,
        checkpoint=checkpoint if details else None,
        checkpoint_every=checkpoint_every,
        existing_records=None,
        max_new_details=None if max_details == 0 else max_details,
    )
    user_event_count = write_jsonl(user_events_output, user_events)

    library_events = read_jsonl(library_events_path, EventRecord)
    user_match_analysis = _build_user_lovelive_event_matches(user_events, library_events)
    _write_user_lovelive_event_match_outputs(
        user_match_analysis,
        user_matches_output,
        user_matches_markdown_output,
        event_ids_output,
    )
    event_ids = [event["event_source_id"] for event in user_match_analysis["matched_events"]]

    matches = read_jsonl(matches_path, MatchRecord)
    setlists = read_jsonl(llfans_path, SetlistRecord)
    group_index = json.loads(group_index_path.read_text(encoding="utf-8"))
    coverage_analysis = _build_song_coverage_analysis(
        event_ids=event_ids,
        events=library_events,
        matches=matches,
        setlists=setlists,
        group_index=group_index,
    )
    coverage_analysis["summary"]["handle"] = handle
    coverage_analysis["summary"]["user_event_count"] = user_match_analysis["summary"]["user_event_count"]
    coverage_analysis["summary"]["matched_lovelive_event_count"] = user_match_analysis["summary"][
        "matched_lovelive_event_count"
    ]
    coverage_analysis["summary"]["unmatched_user_event_count"] = user_match_analysis["summary"][
        "unmatched_user_event_count"
    ]
    coverage_analysis["user_lovelive_event_match"] = user_match_analysis

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(coverage_analysis, ensure_ascii=False, indent=2), encoding="utf-8")
    markdown_output.parent.mkdir(parents=True, exist_ok=True)
    markdown_output.write_text(_render_song_coverage_markdown(coverage_analysis), encoding="utf-8")
    typer.echo(f"Wrote {user_event_count} Eventernote user events to {user_events_output}")
    typer.echo(f"Wrote {len(event_ids)} matched LoveLive event ids to {event_ids_output}")
    typer.echo(f"Wrote handle song coverage analysis to {output}")
    typer.echo(f"Wrote handle song coverage markdown to {markdown_output}")


@app.command("crawl-setlists")
def crawl_setlists(
    events_path: Annotated[Path, typer.Option("--events")] = DEFAULT_EVENT_PATH,
    llfans_output: Annotated[Path, typer.Option("--llfans-output")] = DEFAULT_LLFANS_PATH,
    setlistfm_output: Annotated[Path, typer.Option("--setlistfm-output")] = DEFAULT_SETLISTFM_PATH,
    livefans_output: Annotated[Path, typer.Option("--livefans-output")] = DEFAULT_LIVEFANS_PATH,
    keyword: Annotated[Optional[List[str]], typer.Option("--keyword", "-k")] = None,
    llfans_candidates: Annotated[int, typer.Option("--llfans-candidates")] = 5,
    livefans_max_pages: Annotated[int, typer.Option("--livefans-max-pages")] = 1,
    livefans_event_queries: Annotated[int, typer.Option("--livefans-event-queries")] = 3,
    setlistfm_pages_per_query: Annotated[int, typer.Option("--setlistfm-pages-per-query")] = 1,
    setlistfm_api_key: Annotated[Optional[str], typer.Option("--setlistfm-api-key")] = None,
    skip_llfans: Annotated[bool, typer.Option("--skip-llfans")] = False,
    skip_setlistfm: Annotated[bool, typer.Option("--skip-setlistfm/--with-setlistfm")] = True,
    skip_livefans: Annotated[bool, typer.Option("--skip-livefans/--with-livefans")] = True,
    delay: Annotated[float, typer.Option("--delay")] = 1.0,
    verbose: Annotated[bool, typer.Option("--verbose/--quiet")] = True,
    overwrite: Annotated[bool, typer.Option("--overwrite/--merge")] = False,
) -> None:
    if skip_llfans and skip_livefans and skip_setlistfm:
        raise typer.BadParameter("At least one source must be enabled.")
    _ensure_positive(llfans_candidates, "--llfans-candidates")
    _ensure_non_negative(livefans_max_pages, "--livefans-max-pages")
    _ensure_positive(livefans_event_queries, "--livefans-event-queries")
    _ensure_positive(setlistfm_pages_per_query, "--setlistfm-pages-per-query")
    _ensure_non_negative(delay, "--delay")
    keywords = keyword or ["LoveLive", "ラブライブ"]
    events = read_jsonl(events_path, EventRecord) if events_path.exists() else []

    if not skip_llfans:
        llfans = LLFansClient()
        llfans.fetcher.delay_seconds = delay
        if events:
            llfans_records = llfans.collect_for_events(
                events,
                max_candidate_tours=llfans_candidates,
                progress=typer.echo if verbose else None,
            )
        else:
            llfans_records = []
            tours = llfans.list_tours(progress=typer.echo if verbose else None)
            for index, tour in enumerate(tours, start=1):
                tour_records = llfans.fetch_tour_setlists(tour.source_id)
                llfans_records.extend(tour_records)
                if verbose:
                    typer.echo(f"[llfans] tour {index}/{len(tours)} tour_id={tour.source_id} setlists={len(tour_records)}")
        if overwrite:
            count = write_jsonl(llfans_output, llfans_records)
        else:
            count = merge_jsonl(llfans_output, llfans_records, key=lambda item: item.source_id, model=SetlistRecord)
        typer.echo(f"Wrote {count} LL-Fans setlists to {llfans_output}")

    if not skip_livefans:
        livefans = LiveFansClient()
        livefans.fetcher.delay_seconds = delay
        livefans_pages = None if livefans_max_pages == 0 else livefans_max_pages
        if events:
            livefans_records = livefans.collect_for_events(
                events,
                fallback_keywords=keywords,
                max_pages_per_query=livefans_pages,
                max_queries_per_event=livefans_event_queries,
            )
        else:
            livefans_records = livefans.search_setlists(keywords=keywords, max_pages=livefans_pages)
        if overwrite:
            count = write_jsonl(livefans_output, livefans_records)
        else:
            count = merge_jsonl(livefans_output, livefans_records, key=lambda item: item.source_id, model=SetlistRecord)
        typer.echo(f"Wrote {count} LiveFans setlists to {livefans_output}")

    if not skip_setlistfm:
        api_key = setlistfm_api_key or os.getenv("SETLISTFM_API_KEY")
        if not api_key:
            typer.echo("Skipped setlist.fm: provide --setlistfm-api-key or SETLISTFM_API_KEY.")
        else:
            setlistfm = SetlistFmClient(api_key=api_key)
            setlistfm.fetcher.delay_seconds = delay
            if events:
                setlistfm_records = setlistfm.collect_for_events(events, max_pages_per_query=setlistfm_pages_per_query)
            else:
                setlistfm_records = setlistfm.search_default_artists(max_pages_per_artist=setlistfm_pages_per_query)
            if overwrite:
                count = write_jsonl(setlistfm_output, setlistfm_records)
            else:
                count = merge_jsonl(setlistfm_output, setlistfm_records, key=lambda item: item.source_id, model=SetlistRecord)
            typer.echo(f"Wrote {count} setlist.fm setlists to {setlistfm_output}")


@app.command("crawl-discography")
def crawl_discography(
    output: Annotated[Path, typer.Option("--output", "-o")] = DEFAULT_DISCOGRAPHY_SONGS_PATH,
    group_index_output: Annotated[Path, typer.Option("--group-index-output")] = DEFAULT_GROUP_SONG_INDEX_PATH,
    delay: Annotated[float, typer.Option("--delay")] = 0.2,
    verbose: Annotated[bool, typer.Option("--verbose/--quiet")] = True,
) -> None:
    _ensure_non_negative(delay, "--delay")
    client = LLFansClient()
    client.fetcher.delay_seconds = delay
    records = client.collect_discography_songs(progress=typer.echo if verbose else None)
    count = write_jsonl(output, records)
    group_index = _build_group_song_index(records)
    group_index_output.parent.mkdir(parents=True, exist_ok=True)
    group_index_output.write_text(json.dumps(group_index, ensure_ascii=False, indent=2), encoding="utf-8")
    typer.echo(f"Wrote {count} LL-Fans discography song rows to {output}")
    typer.echo(f"Wrote group song index to {group_index_output}")


@app.command("analyze-song-coverage")
def analyze_song_coverage(
    event_id: Annotated[Optional[List[str]], typer.Option("--event-id", help="Eventernote event id, repeatable.")] = None,
    event_ids_file: Annotated[Optional[Path], typer.Option("--event-ids-file", help="Text file with one Eventernote event id per line.")] = None,
    events_path: Annotated[Path, typer.Option("--events")] = Path("data/raw/eventernote_main_groups_events.jsonl"),
    matches_path: Annotated[Path, typer.Option("--matches")] = Path("data/processed/main_groups_event_setlist_matches.jsonl"),
    llfans_path: Annotated[Path, typer.Option("--llfans")] = Path("data/raw/llfans_all_setlists.jsonl"),
    group_index_path: Annotated[Path, typer.Option("--group-index")] = DEFAULT_GROUP_SONG_INDEX_PATH,
    output: Annotated[Path, typer.Option("--output", "-o")] = DEFAULT_SONG_COVERAGE_JSON_PATH,
    markdown_output: Annotated[Path, typer.Option("--markdown-output")] = DEFAULT_SONG_COVERAGE_MARKDOWN_PATH,
) -> None:
    event_ids = _load_event_ids(event_id or [], event_ids_file)
    if not event_ids:
        raise typer.BadParameter("Provide at least one --event-id or --event-ids-file.")
    _ensure_existing_file(events_path, "--events")
    _ensure_existing_file(matches_path, "--matches")
    _ensure_existing_file(llfans_path, "--llfans")
    _ensure_existing_file(group_index_path, "--group-index")

    events = read_jsonl(events_path, EventRecord)
    matches = read_jsonl(matches_path, MatchRecord)
    setlists = read_jsonl(llfans_path, SetlistRecord)
    group_index = json.loads(group_index_path.read_text(encoding="utf-8"))

    analysis = _build_song_coverage_analysis(
        event_ids=event_ids,
        events=events,
        matches=matches,
        setlists=setlists,
        group_index=group_index,
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(analysis, ensure_ascii=False, indent=2), encoding="utf-8")
    markdown_output.parent.mkdir(parents=True, exist_ok=True)
    markdown_output.write_text(_render_song_coverage_markdown(analysis), encoding="utf-8")
    typer.echo(f"Wrote song coverage analysis to {output}")
    typer.echo(f"Wrote song coverage markdown to {markdown_output}")


@app.command("match")
def match(
    events_path: Annotated[Path, typer.Option("--events")] = DEFAULT_EVENT_PATH,
    llfans_path: Annotated[Path, typer.Option("--llfans")] = DEFAULT_LLFANS_PATH,
    setlistfm_path: Annotated[Path, typer.Option("--setlistfm")] = DEFAULT_SETLISTFM_PATH,
    livefans_path: Annotated[Path, typer.Option("--livefans")] = DEFAULT_LIVEFANS_PATH,
    output: Annotated[Path, typer.Option("--output", "-o")] = DEFAULT_MATCH_PATH,
    threshold: Annotated[float, typer.Option("--threshold")] = 65.0,
    top_n: Annotated[int, typer.Option("--top-n")] = 3,
) -> None:
    _ensure_existing_file(events_path, "--events")
    if not llfans_path.exists() and not setlistfm_path.exists() and not livefans_path.exists():
        raise typer.BadParameter("At least one setlist input must exist.")
    if threshold < 0 or threshold > 100:
        raise typer.BadParameter("--threshold must be between 0 and 100")
    _ensure_positive(top_n, "--top-n")
    events = read_jsonl(events_path, EventRecord)
    setlists = [
        *read_jsonl(llfans_path, SetlistRecord),
        *read_jsonl(setlistfm_path, SetlistRecord),
        *read_jsonl(livefans_path, SetlistRecord),
    ]
    matches = match_events_to_setlists(events, setlists, threshold=threshold, top_n=top_n)
    count = write_jsonl(output, matches)
    typer.echo(f"Wrote {count} matches to {output}")


@app.command("export-markdown")
def export_markdown(
    events_path: Annotated[Path, typer.Option("--events")] = DEFAULT_EVENT_PATH,
    llfans_path: Annotated[Path, typer.Option("--llfans")] = DEFAULT_LLFANS_PATH,
    setlistfm_path: Annotated[Path, typer.Option("--setlistfm")] = DEFAULT_SETLISTFM_PATH,
    livefans_path: Annotated[Path, typer.Option("--livefans")] = DEFAULT_LIVEFANS_PATH,
    matches_path: Annotated[Path, typer.Option("--matches")] = DEFAULT_MATCH_PATH,
    output: Annotated[Path, typer.Option("--output", "-o")] = DEFAULT_EXPORT_PATH,
) -> None:
    _ensure_existing_file(events_path, "--events")
    _ensure_existing_file(matches_path, "--matches")
    if not llfans_path.exists() and not setlistfm_path.exists() and not livefans_path.exists():
        raise typer.BadParameter("At least one setlist input must exist.")
    events = read_jsonl(events_path, EventRecord)
    setlists = [
        *read_jsonl(llfans_path, SetlistRecord),
        *read_jsonl(setlistfm_path, SetlistRecord),
        *read_jsonl(livefans_path, SetlistRecord),
    ]
    matches = read_jsonl(matches_path, MatchRecord)
    count = export_matches_markdown(output, events, setlists, matches)
    typer.echo(f"Wrote {count} match entries to {output}")


@app.command("audit-coverage")
def audit_coverage(
    events_path: Annotated[Path, typer.Option("--events")] = DEFAULT_EVENT_PATH,
    llfans_path: Annotated[Path, typer.Option("--llfans")] = DEFAULT_LLFANS_PATH,
    setlistfm_path: Annotated[Path, typer.Option("--setlistfm")] = DEFAULT_SETLISTFM_PATH,
    livefans_path: Annotated[Path, typer.Option("--livefans")] = DEFAULT_LIVEFANS_PATH,
    matches_path: Annotated[Path, typer.Option("--matches")] = DEFAULT_MATCH_PATH,
    output: Annotated[Path, typer.Option("--output", "-o")] = Path("data/processed/setlist_coverage_audit.md"),
) -> None:
    _ensure_existing_file(events_path, "--events")
    _ensure_existing_file(matches_path, "--matches")
    events = read_jsonl(events_path, EventRecord)
    setlists = [
        *read_jsonl(llfans_path, SetlistRecord),
        *read_jsonl(setlistfm_path, SetlistRecord),
        *read_jsonl(livefans_path, SetlistRecord),
    ]
    matches = read_jsonl(matches_path, MatchRecord)
    matched_event_ids = {match.event_source_id for match in matches}
    unmatched = [event for event in events if event.source_id not in matched_event_ids]

    lines = [
        "# Setlist Coverage Audit",
        "",
        f"- Events: {len(events)}",
        f"- Setlists: {len(setlists)}",
        f"- Matched events: {len(matched_event_ids)}",
        f"- Unmatched events: {len(unmatched)}",
        "",
        "## Unmatched Events",
        "",
    ]
    for event in unmatched:
        reason, candidate = _diagnose_unmatched(event, setlists)
        candidate_text = ""
        if candidate:
            candidate_text = (
                f" Best candidate: [{candidate.source}] {candidate.title} "
                f"date={candidate.event_date or 'unknown'} start={candidate.start_time or 'unknown'}."
            )
        lines.append(
            f"- {event.event_date or 'unknown'} {event.start_time or '--:--'} "
            f"`{event.source_id}` {event.title} - {reason}.{candidate_text}"
        )
    if not unmatched:
        lines.append("- none")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(lines), encoding="utf-8")
    typer.echo(f"Wrote coverage audit to {output}")


def _diagnose_unmatched(event: EventRecord, setlists: list[SetlistRecord]) -> tuple[str, SetlistRecord | None]:
    if not event.event_date:
        return "event_date_missing", None
    same_date = [setlist for setlist in setlists if setlist.event_date == event.event_date]
    if not same_date:
        return "no_setlist_same_date", None
    if event.start_time:
        same_start = [setlist for setlist in same_date if setlist.start_time == event.start_time]
        if not same_start:
            return "no_setlist_same_start_time", _best_title_candidate(event, same_date)
        return "below_threshold_same_start_time", _best_title_candidate(event, same_start)
    return "below_threshold_same_date", _best_title_candidate(event, same_date)


def _build_user_lovelive_event_matches(user_events: list[EventRecord], library_events: list[EventRecord]) -> dict:
    library_by_id = {event.source_id: event for event in library_events}
    matched_events = []
    group_counts: dict[str, int] = defaultdict(int)
    for user_event in user_events:
        library_event = library_by_id.get(user_event.source_id)
        if not library_event:
            continue
        groups = _main_group_names_for_event(library_event)
        for group_name in groups:
            group_counts[group_name] += 1
        matched_events.append(
            {
                **_event_summary(library_event),
                "groups": groups,
                "performers": library_event.performers,
                "library_keywords": library_event.keywords,
                "user_event": _event_summary(user_event),
            }
        )
    matched_events.sort(
        key=lambda item: (
            item.get("event_date") or "9999-99-99",
            item.get("start_time") or "99:99",
            item.get("event_source_id") or "",
        )
    )
    return {
        "summary": {
            "user_event_count": len(user_events),
            "library_event_count": len(library_events),
            "matched_lovelive_event_count": len(matched_events),
            "unmatched_user_event_count": len(user_events) - len(matched_events),
            "group_counts": dict(sorted(group_counts.items())),
        },
        "matched_events": matched_events,
    }


def _main_group_names_for_event(event: EventRecord) -> list[str]:
    groups = []
    performer_names = set(event.performers)
    keyword_values = set(event.keywords)
    for group_name, actor_url in MAIN_GROUP_ACTOR_URLS.items():
        if actor_url in keyword_values or group_name in performer_names or group_name in event.title:
            groups.append(group_name)
    return groups


def _render_user_lovelive_matches_markdown(analysis: dict) -> str:
    summary = analysis["summary"]
    lines = [
        "# User LoveLive Event Matches",
        "",
        f"- User events: {summary['user_event_count']}",
        f"- Library events: {summary['library_event_count']}",
        f"- Matched LoveLive events: {summary['matched_lovelive_event_count']}",
        f"- Unmatched user events: {summary['unmatched_user_event_count']}",
        "",
        "## Group Summary",
        "",
    ]
    group_counts = summary.get("group_counts", {})
    if group_counts:
        for group_name, count in group_counts.items():
            lines.append(f"- {group_name}: {count}")
    else:
        lines.append("- none")

    lines.extend(["", "## Matched Events", ""])
    if analysis["matched_events"]:
        for event in analysis["matched_events"]:
            groups = ", ".join(event.get("groups") or []) or "unknown"
            lines.append(
                f"- {event.get('event_date') or 'unknown'} {event.get('start_time') or '--:--'} "
                f"`{event['event_source_id']}` {event['title']} - {groups}"
            )
    else:
        lines.append("- none")
    return "\n".join(lines) + "\n"


def _write_user_lovelive_event_match_outputs(
    analysis: dict,
    output: Path,
    markdown_output: Path,
    event_ids_output: Path,
) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(analysis, ensure_ascii=False, indent=2), encoding="utf-8")
    markdown_output.parent.mkdir(parents=True, exist_ok=True)
    markdown_output.write_text(_render_user_lovelive_matches_markdown(analysis), encoding="utf-8")
    event_ids_output.parent.mkdir(parents=True, exist_ok=True)
    event_ids_output.write_text(
        "\n".join(event["event_source_id"] for event in analysis["matched_events"]) + "\n",
        encoding="utf-8",
    )


def _load_event_ids(event_ids: list[str], event_ids_file: Path | None) -> list[str]:
    values = list(event_ids)
    if event_ids_file:
        _ensure_existing_file(event_ids_file, "--event-ids-file")
        values.extend(
            line.split("#", 1)[0].strip()
            for line in event_ids_file.read_text(encoding="utf-8").splitlines()
        )
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        cleaned = value.strip()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        result.append(cleaned)
    return result


def _build_song_coverage_analysis(
    event_ids: list[str],
    events: list[EventRecord],
    matches: list[MatchRecord],
    setlists: list[SetlistRecord],
    group_index: dict,
) -> dict:
    events_by_id = {event.source_id: event for event in events}
    matches_by_event: dict[str, list[MatchRecord]] = defaultdict(list)
    for match in matches:
        matches_by_event[match.event_source_id].append(match)
    setlists_by_key = {(setlist.source, setlist.source_id): setlist for setlist in setlists}
    song_master = _song_master_by_id(group_index)

    heard_by_group: dict[str, dict[str, dict]] = defaultdict(dict)
    input_events = []
    unknown_event_ids = []
    events_without_match = []
    missing_setlists = []
    unassigned_songs = []
    matched_setlist_keys: set[tuple[str, str]] = set()

    for source_id in event_ids:
        event = events_by_id.get(source_id)
        if not event:
            unknown_event_ids.append(source_id)
            continue
        event_matches = sorted(matches_by_event.get(source_id, []), key=lambda item: item.match_score, reverse=True)
        if not event_matches:
            events_without_match.append(_event_summary(event))
        matched_setlists = []
        event_heard_song_ids: set[str] = set()
        for match in event_matches:
            setlist = setlists_by_key.get((match.setlist_source, match.setlist_source_id))
            if not setlist:
                missing_setlists.append(
                    {
                        "event_source_id": source_id,
                        "setlist_source": match.setlist_source,
                        "setlist_source_id": match.setlist_source_id,
                        "setlist_title": match.setlist_title,
                    }
                )
                continue
            matched_setlist_keys.add((setlist.source, setlist.source_id))
            matched_setlists.append(
                {
                    "source": setlist.source,
                    "source_id": setlist.source_id,
                    "title": setlist.title,
                    "url": setlist.url,
                    "event_date": setlist.event_date,
                    "start_time": setlist.start_time,
                    "match_score": match.match_score,
                }
            )
            for song in setlist.songs:
                if not song.source_song_id or song.source_song_id not in song_master:
                    unassigned_songs.append(
                        {
                            "event_source_id": source_id,
                            "event_title": event.title,
                            "setlist_source_id": setlist.source_id,
                            "setlist_title": setlist.title,
                            "song_title": song.title,
                            "source_song_id": song.source_song_id,
                        }
                    )
                    continue
                event_heard_song_ids.add(song.source_song_id)
                for group_name in song_master[song.source_song_id]["groups"]:
                    group_songs = heard_by_group[group_name]
                    heard = group_songs.setdefault(
                        song.source_song_id,
                        {
                            "source_song_id": song.source_song_id,
                            "title": song_master[song.source_song_id]["title"],
                            "appearances": 0,
                            "events_by_id": {},
                        },
                    )
                    heard["appearances"] += 1
                    heard["events_by_id"].setdefault(
                        source_id,
                        {
                            **_event_summary(event),
                            "setlists": [],
                        },
                    )
                    heard["events_by_id"][source_id]["setlists"].append(
                        {
                            "source": setlist.source,
                            "source_id": setlist.source_id,
                            "title": setlist.title,
                            "url": setlist.url,
                        }
                    )
        input_events.append(
            {
                **_event_summary(event),
                "matched_setlists": matched_setlists,
                "heard_song_count": len(event_heard_song_ids),
            }
        )

    groups = {}
    total_songs = 0
    heard_song_ids_global: set[str] = set()
    for group_name, group_data in sorted(group_index.get("groups", {}).items()):
        master_songs = {song["source_song_id"]: song for song in group_data.get("songs", [])}
        heard_songs = []
        for song_id, heard in heard_by_group.get(group_name, {}).items():
            master_song = master_songs.get(song_id)
            if not master_song:
                continue
            events_list = sorted(
                heard["events_by_id"].values(),
                key=lambda item: (item.get("event_date") or "9999-99-99", item.get("event_source_id") or ""),
            )
            heard_song_ids_global.add(song_id)
            heard_songs.append(
                {
                    "source_song_id": song_id,
                    "title": heard["title"],
                    "appearances": heard["appearances"],
                    "event_count": len(events_list),
                    "first_heard_at": events_list[0].get("event_date") if events_list else None,
                    "first_heard_event": events_list[0] if events_list else None,
                    "events": events_list,
                    "master": master_song,
                }
            )
        heard_songs.sort(key=lambda item: (item["first_heard_at"] or "9999-99-99", item["title"]))
        heard_ids = {song["source_song_id"] for song in heard_songs}
        unheard_songs = [song for song_id, song in master_songs.items() if song_id not in heard_ids]
        unheard_songs.sort(key=lambda item: (item.get("first_released_at") or "9999-99-99", item["title"]))
        total_count = len(master_songs)
        heard_count = len(heard_songs)
        total_songs += total_count
        groups[group_name] = {
            "total_count": total_count,
            "heard_count": heard_count,
            "unheard_count": total_count - heard_count,
            "coverage_percent": round(heard_count / total_count * 100, 2) if total_count else 0.0,
            "heard_songs": heard_songs,
            "unheard_songs": unheard_songs,
        }

    return {
        "summary": {
            "input_event_ids": event_ids,
            "input_event_count": len(event_ids),
            "known_event_count": len(input_events),
            "matched_event_count": sum(1 for event in input_events if event["matched_setlists"]),
            "matched_setlist_count": len(matched_setlist_keys),
            "unique_heard_song_count": len(heard_song_ids_global),
            "total_group_song_count": total_songs,
        },
        "groups": groups,
        "input_events": input_events,
        "issues": {
            "unknown_event_ids": unknown_event_ids,
            "events_without_match": events_without_match,
            "missing_setlists": missing_setlists,
            "unassigned_songs": unassigned_songs,
        },
    }


def _song_master_by_id(group_index: dict) -> dict[str, dict]:
    songs: dict[str, dict] = {}
    for group_name, group_data in group_index.get("groups", {}).items():
        for song in group_data.get("songs", []):
            source_song_id = song["source_song_id"]
            item = songs.setdefault(
                source_song_id,
                {
                    "source_song_id": source_song_id,
                    "title": song["title"],
                    "groups": [],
                    "master_by_group": {},
                },
            )
            item["groups"].append(group_name)
            item["master_by_group"][group_name] = song
    return songs


def _event_summary(event: EventRecord) -> dict:
    return {
        "event_source_id": event.source_id,
        "title": event.title,
        "url": event.url,
        "event_date": event.event_date,
        "start_time": event.start_time,
        "venue": event.venue,
    }


def _render_song_coverage_markdown(analysis: dict) -> str:
    summary = analysis["summary"]
    lines = [
        "# LoveLive Song Coverage Analysis",
        "",
        f"- Input events: {summary['input_event_count']}",
        f"- Known events: {summary['known_event_count']}",
        f"- Matched events: {summary['matched_event_count']}",
        f"- Matched setlists: {summary['matched_setlist_count']}",
        f"- Unique heard songs: {summary['unique_heard_song_count']}",
        "",
        "## Group Summary",
        "",
        "| Group | Heard | Total | Coverage |",
        "| --- | ---: | ---: | ---: |",
    ]
    for group_name, group in analysis["groups"].items():
        lines.append(
            f"| {group_name} | {group['heard_count']} | {group['total_count']} | "
            f"{group['coverage_percent']:.2f}% |"
        )

    lines.extend(["", "## Input Events", ""])
    for event in analysis["input_events"]:
        lines.append(
            f"- {event.get('event_date') or 'unknown'} `{event['event_source_id']}` "
            f"{event['title']} - setlists={len(event['matched_setlists'])}, heard_songs={event['heard_song_count']}"
        )

    for group_name, group in analysis["groups"].items():
        lines.extend(
            [
                "",
                f"## {group_name}",
                "",
                f"- Coverage: {group['heard_count']} / {group['total_count']} ({group['coverage_percent']:.2f}%)",
                "",
                "### Heard Songs",
                "",
            ]
        )
        if group["heard_songs"]:
            for song in group["heard_songs"]:
                first_event = song.get("first_heard_event") or {}
                lines.append(
                    f"- `{song['source_song_id']}` {song['title']} - "
                    f"events={song['event_count']}, appearances={song['appearances']}, "
                    f"first={song.get('first_heard_at') or 'unknown'} "
                    f"`{first_event.get('event_source_id', 'unknown')}` {first_event.get('title', '')}"
                )
        else:
            lines.append("- none")

        lines.extend(["", "### Unheard Songs", ""])
        if group["unheard_songs"]:
            for song in group["unheard_songs"]:
                discographies = song.get("discographies") or []
                source = discographies[0] if discographies else {}
                lines.append(
                    f"- `{song['source_song_id']}` {song['title']} - "
                    f"released={song.get('first_released_at') or 'unknown'}, "
                    f"source={source.get('title', 'unknown')}"
                )
        else:
            lines.append("- none")

    issues = analysis["issues"]
    lines.extend(["", "## Issues", ""])
    lines.append(f"- Unknown event ids: {len(issues['unknown_event_ids'])}")
    for source_id in issues["unknown_event_ids"]:
        lines.append(f"  - `{source_id}`")
    lines.append(f"- Events without match: {len(issues['events_without_match'])}")
    for event in issues["events_without_match"]:
        lines.append(f"  - `{event['event_source_id']}` {event['title']}")
    lines.append(f"- Missing setlists: {len(issues['missing_setlists'])}")
    lines.append(f"- Unassigned songs: {len(issues['unassigned_songs'])}")
    return "\n".join(lines) + "\n"


def _build_group_song_index(records: list[DiscographySongRecord]) -> dict:
    groups: dict[str, dict[str, dict]] = defaultdict(dict)
    for record in records:
        group_names = record.group_names or record.series_names or ["Unknown"]
        for group_name in group_names:
            songs = groups[group_name]
            song = songs.setdefault(
                record.source_song_id,
                {
                    "source_song_id": record.source_song_id,
                    "title": record.title,
                    "series_ids": record.series_ids,
                    "series_names": record.series_names,
                    "appearances": 0,
                    "discography_ids": set(),
                    "discographies": [],
                    "first_released_at": record.released_at,
                    "last_released_at": record.released_at,
                },
            )
            song["appearances"] += 1
            if record.released_at and (
                song["first_released_at"] is None or record.released_at < song["first_released_at"]
            ):
                song["first_released_at"] = record.released_at
            if record.released_at and (
                song["last_released_at"] is None or record.released_at > song["last_released_at"]
            ):
                song["last_released_at"] = record.released_at
            if record.discography_id not in song["discography_ids"]:
                song["discography_ids"].add(record.discography_id)
                song["discographies"].append(
                    {
                        "discography_id": record.discography_id,
                        "title": record.discography_title,
                        "released_at": record.released_at,
                    }
                )

    result_groups = {}
    for group_name, songs_by_id in sorted(groups.items()):
        songs = []
        for song in songs_by_id.values():
            song = dict(song)
            song["discography_count"] = len(song.pop("discography_ids"))
            song["discographies"].sort(key=lambda item: (item["released_at"] or "9999-99-99", item["title"]))
            songs.append(song)
        songs.sort(key=lambda item: (item["first_released_at"] or "9999-99-99", item["title"]))
        result_groups[group_name] = {
            "unique_songs": len(songs),
            "songs": songs,
        }

    return {
        "groups": result_groups,
    }


def _best_title_candidate(event: EventRecord, setlists: list[SetlistRecord]) -> SetlistRecord | None:
    if not setlists:
        return None
    return max(
        setlists,
        key=lambda setlist: fuzz.token_set_ratio(normalize_text(event.title), normalize_text(setlist.title)),
    )

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


class SourceLink(BaseModel):
    label: str | None = None
    url: str


class SongRecord(BaseModel):
    position: int | None = None
    title: str
    artist: str | None = None
    source_song_id: str | None = None
    raw: dict[str, Any] = Field(default_factory=dict)


class DiscographySongRecord(BaseModel):
    source: Literal["llfans"] = "llfans"
    source_id: str
    source_song_id: str
    title: str
    url: str
    series_ids: list[int] = Field(default_factory=list)
    series_names: list[str] = Field(default_factory=list)
    group_names: list[str] = Field(default_factory=list)
    discography_id: str
    discography_title: str
    discography_description: str | None = None
    released_at: str | None = None
    discography_type_id: str | None = None
    version_id: str | None = None
    version_name: str | None = None
    disc_id: str | None = None
    disc_number: int | None = None
    disc_track_id: str | None = None
    song_version_id: str | None = None
    song_version_name: str | None = None
    fetched_at: datetime = Field(default_factory=datetime.utcnow)
    raw: dict[str, Any] = Field(default_factory=dict)


class EventRecord(BaseModel):
    source: Literal["eventernote"] = "eventernote"
    source_id: str
    title: str
    url: str
    event_date: str | None = None
    open_time: str | None = None
    start_time: str | None = None
    end_time: str | None = None
    venue: str | None = None
    venue_url: str | None = None
    performers: list[str] = Field(default_factory=list)
    related_links: list[SourceLink] = Field(default_factory=list)
    keywords: list[str] = Field(default_factory=list)
    attendee_count: int | None = None
    description: str | None = None
    raw_text: str | None = None
    fetched_at: datetime = Field(default_factory=datetime.utcnow)


class SetlistRecord(BaseModel):
    source: Literal["setlistfm", "livefans", "llfans"]
    source_id: str
    title: str
    url: str
    event_date: str | None = None
    start_time: str | None = None
    venue: str | None = None
    venue_url: str | None = None
    artists: list[str] = Field(default_factory=list)
    tour: str | None = None
    songs: list[SongRecord] = Field(default_factory=list)
    raw_text: str | None = None
    fetched_at: datetime = Field(default_factory=datetime.utcnow)
    raw: dict[str, Any] = Field(default_factory=dict)


class MatchRecord(BaseModel):
    event_source_id: str
    setlist_source: Literal["setlistfm", "livefans", "llfans"]
    setlist_source_id: str
    match_score: float
    match_reason: list[str] = Field(default_factory=list)
    event_url: str
    setlist_url: str
    event_title: str
    setlist_title: str
    event_date: str | None = None
    setlist_date: str | None = None
    event_venue: str | None = None
    setlist_venue: str | None = None
    reviewed: bool = False

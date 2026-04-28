import { promises as fs } from "node:fs";
import path from "node:path";
import * as cheerio from "cheerio";

const BASE_URL = "https://www.eventernote.com";
const EVENT_RE = /^\/events\/(?<id>\d+)$/;
const NEXT_LABELS = new Set(["次へ", "Next", ">", "»"]);
const DATA_ROOT = path.join(process.cwd(), "data");

const MAIN_GROUP_ACTOR_URLS: Record<string, string> = {
  Aqours: "https://www.eventernote.com/actors/Aqours/15434/events",
  "μ’s": "https://www.eventernote.com/actors/%CE%BC%E2%80%99s/2809/events",
  虹ヶ咲学園スクールアイドル同好会:
    "https://www.eventernote.com/actors/%E8%99%B9%E3%83%B6%E5%92%B2%E5%AD%A6%E5%9C%92%E3%82%B9%E3%82%AF%E3%83%BC%E3%83%AB%E3%82%A2%E3%82%A4%E3%83%89%E3%83%AB%E5%90%8C%E5%A5%BD%E4%BC%9A/31600/events",
  "Liella!": "https://www.eventernote.com/actors/Liella%21/59030/events",
  蓮ノ空女学院スクールアイドルクラブ:
    "https://www.eventernote.com/actors/%E8%93%AE%E3%83%8E%E7%A9%BA%E5%A5%B3%E5%AD%A6%E9%99%A2%E3%82%B9%E3%82%AF%E3%83%BC%E3%83%AB%E3%82%A2%E3%82%A4%E3%83%89%E3%83%AB%E3%82%AF%E3%83%A9%E3%83%96/70475/events",
  "いきづらい部！":
    "https://www.eventernote.com/actors/%E3%81%84%E3%81%8D%E3%81%A5%E3%82%89%E3%81%84%E9%83%A8%EF%BC%81/85084/events",
};

type EventRecord = {
  source_id: string;
  title: string;
  url: string;
  event_date?: string | null;
  start_time?: string | null;
  venue?: string | null;
  performers?: string[];
  keywords?: string[];
};

type MatchRecord = {
  event_source_id: string;
  setlist_source: string;
  setlist_source_id: string;
  match_score: number;
  setlist_title: string;
};

type SongRecord = {
  title: string;
  source_song_id?: string | null;
};

type SetlistRecord = {
  source: string;
  source_id: string;
  title: string;
  url: string;
  event_date?: string | null;
  start_time?: string | null;
  songs: SongRecord[];
};

type GroupSong = {
  source_song_id: string;
  title: string;
  first_released_at?: string | null;
  discographies?: Array<{ title?: string }>;
};

type GroupSongIndex = {
  groups: Record<string, { unique_songs: number; songs: GroupSong[] }>;
};

type DataStore = {
  events: EventRecord[];
  matches: MatchRecord[];
  setlists: SetlistRecord[];
  groupIndex: GroupSongIndex;
};

type EventSummary = {
  event_source_id: string;
  title: string;
  url: string;
  event_date?: string | null;
  start_time?: string | null;
  venue?: string | null;
};

type UserEventMatch = EventSummary & {
  groups: string[];
  performers: string[];
  library_keywords: string[];
};

let dataStorePromise: Promise<DataStore> | null = null;

export async function buildCoverageForHandle(handle: string, maxPages = 30) {
  const normalizedHandle = normalizeHandle(handle);
  const [{ eventIds, pageCount }, store] = await Promise.all([
    fetchUserEventIds(normalizedHandle, maxPages),
    loadDataStore(),
  ]);
  const userMatchAnalysis = buildUserLoveLiveEventMatches(eventIds, store.events);
  const coverage = buildSongCoverageAnalysis({
    eventIds: userMatchAnalysis.matched_events.map((event) => event.event_source_id),
    events: store.events,
    matches: store.matches,
    setlists: store.setlists,
    groupIndex: store.groupIndex,
  });

  return {
    ...coverage,
    generated_at: new Date().toISOString(),
    summary: {
      ...coverage.summary,
      handle: normalizedHandle,
      user_event_count: eventIds.length,
      user_event_page_count: pageCount,
      matched_lovelive_event_count: userMatchAnalysis.summary.matched_lovelive_event_count,
      unmatched_user_event_count: eventIds.length - userMatchAnalysis.summary.matched_lovelive_event_count,
    },
    user_lovelive_event_match: userMatchAnalysis,
  };
}

async function fetchUserEventIds(handle: string, maxPages: number) {
  let nextUrl: string | null = `${BASE_URL}/users/${encodeURIComponent(handle)}/events`;
  const expectedPath = `/users/${handle}/events`;
  const visitedPages = new Set<string>();
  const eventIds: string[] = [];
  const seenEventIds = new Set<string>();
  let pageCount = 0;

  while (nextUrl && !visitedPages.has(nextUrl) && pageCount < maxPages) {
    visitedPages.add(nextUrl);
    pageCount += 1;
    const response = await fetch(nextUrl, {
      headers: {
        "user-agent": "senior-lovelive-benchmark/0.1 (+https://github.com/MoonOld/senior_lovelive_benchmark)",
      },
      cache: "no-store",
    });
    if (!response.ok) {
      throw new Error(`Eventernote returned ${response.status} for ${nextUrl}`);
    }
    const html = await response.text();
    const $ = cheerio.load(html);

    $("a[href]").each((_, element) => {
      const href = $(element).attr("href");
      if (!href) {
        return;
      }
      const parsed = new URL(href, BASE_URL);
      const match = decodeURI(parsed.pathname).match(EVENT_RE);
      const eventId = match?.groups?.id;
      if (!eventId || seenEventIds.has(eventId)) {
        return;
      }
      seenEventIds.add(eventId);
      eventIds.push(eventId);
    });

    nextUrl = null;
    $("a[href]").each((_, element) => {
      if (nextUrl) {
        return;
      }
      const label = cleanText($(element).text());
      const href = $(element).attr("href");
      if (!href || !NEXT_LABELS.has(label)) {
        return;
      }
      const parsed = new URL(href, BASE_URL);
      if (decodeURI(parsed.pathname) === expectedPath) {
        nextUrl = parsed.toString();
      }
    });
  }

  return { eventIds, pageCount };
}

function buildUserLoveLiveEventMatches(userEventIds: string[], libraryEvents: EventRecord[]) {
  const libraryById = new Map(libraryEvents.map((event) => [event.source_id, event]));
  const matchedEvents: UserEventMatch[] = [];
  const groupCounts: Record<string, number> = {};

  for (const eventId of userEventIds) {
    const event = libraryById.get(eventId);
    if (!event) {
      continue;
    }
    const groups = mainGroupNamesForEvent(event);
    for (const groupName of groups) {
      groupCounts[groupName] = (groupCounts[groupName] ?? 0) + 1;
    }
    matchedEvents.push({
      ...eventSummary(event),
      groups,
      performers: event.performers ?? [],
      library_keywords: event.keywords ?? [],
    });
  }

  matchedEvents.sort((left, right) =>
    [
      (left.event_date ?? "9999-99-99").localeCompare(right.event_date ?? "9999-99-99"),
      (left.start_time ?? "99:99").localeCompare(right.start_time ?? "99:99"),
      left.event_source_id.localeCompare(right.event_source_id),
    ].find((value) => value !== 0) ?? 0,
  );

  return {
    summary: {
      user_event_count: userEventIds.length,
      library_event_count: libraryEvents.length,
      matched_lovelive_event_count: matchedEvents.length,
      unmatched_user_event_count: userEventIds.length - matchedEvents.length,
      group_counts: Object.fromEntries(Object.entries(groupCounts).sort(([left], [right]) => left.localeCompare(right))),
    },
    matched_events: matchedEvents,
  };
}

function buildSongCoverageAnalysis({
  eventIds,
  events,
  matches,
  setlists,
  groupIndex,
}: {
  eventIds: string[];
  events: EventRecord[];
  matches: MatchRecord[];
  setlists: SetlistRecord[];
  groupIndex: GroupSongIndex;
}) {
  const eventsById = new Map(events.map((event) => [event.source_id, event]));
  const matchesByEvent = new Map<string, MatchRecord[]>();
  for (const match of matches) {
    const eventMatches = matchesByEvent.get(match.event_source_id) ?? [];
    eventMatches.push(match);
    matchesByEvent.set(match.event_source_id, eventMatches);
  }
  const setlistsByKey = new Map(setlists.map((setlist) => [`${setlist.source}:${setlist.source_id}`, setlist]));
  const songMaster = songMasterById(groupIndex);
  const heardByGroup = new Map<string, Map<string, HeardSong>>();
  const inputEvents = [];
  const unknownEventIds = [];
  const eventsWithoutMatch = [];
  const missingSetlists = [];
  const unassignedSongs = [];
  const matchedSetlistKeys = new Set<string>();

  for (const eventId of eventIds) {
    const event = eventsById.get(eventId);
    if (!event) {
      unknownEventIds.push(eventId);
      continue;
    }
    const eventMatches = [...(matchesByEvent.get(eventId) ?? [])].sort((left, right) => right.match_score - left.match_score);
    if (eventMatches.length === 0) {
      eventsWithoutMatch.push(eventSummary(event));
    }
    const matchedSetlists = [];
    const eventHeardSongIds = new Set<string>();

    for (const match of eventMatches) {
      const setlistKey = `${match.setlist_source}:${match.setlist_source_id}`;
      const setlist = setlistsByKey.get(setlistKey);
      if (!setlist) {
        missingSetlists.push({
          event_source_id: eventId,
          setlist_source: match.setlist_source,
          setlist_source_id: match.setlist_source_id,
          setlist_title: match.setlist_title,
        });
        continue;
      }
      matchedSetlistKeys.add(setlistKey);
      matchedSetlists.push({
        source: setlist.source,
        source_id: setlist.source_id,
        title: setlist.title,
        url: setlist.url,
        event_date: setlist.event_date,
        start_time: setlist.start_time,
        match_score: match.match_score,
      });

      for (const song of setlist.songs) {
        if (!song.source_song_id || !songMaster[song.source_song_id]) {
          unassignedSongs.push({
            event_source_id: eventId,
            event_title: event.title,
            setlist_source_id: setlist.source_id,
            setlist_title: setlist.title,
            song_title: song.title,
            source_song_id: song.source_song_id,
          });
          continue;
        }
        eventHeardSongIds.add(song.source_song_id);
        for (const groupName of songMaster[song.source_song_id].groups) {
          const groupSongs = heardByGroup.get(groupName) ?? new Map<string, HeardSong>();
          const heard =
            groupSongs.get(song.source_song_id) ??
            ({
              source_song_id: song.source_song_id,
              title: songMaster[song.source_song_id].title,
              appearances: 0,
              events_by_id: {},
            } satisfies HeardSong);
          heard.appearances += 1;
          heard.events_by_id[eventId] ??= {
            ...eventSummary(event),
            setlists: [],
          };
          heard.events_by_id[eventId].setlists.push({
            source: setlist.source,
            source_id: setlist.source_id,
            title: setlist.title,
            url: setlist.url,
          });
          groupSongs.set(song.source_song_id, heard);
          heardByGroup.set(groupName, groupSongs);
        }
      }
    }

    inputEvents.push({
      ...eventSummary(event),
      matched_setlists: matchedSetlists,
      heard_song_count: eventHeardSongIds.size,
    });
  }

  const groups: Record<string, unknown> = {};
  let totalSongs = 0;
  const heardSongIdsGlobal = new Set<string>();

  for (const [groupName, groupData] of Object.entries(groupIndex.groups).sort(([left], [right]) => left.localeCompare(right))) {
    const masterSongs = Object.fromEntries(groupData.songs.map((song) => [song.source_song_id, song]));
    const heardSongs = [];
    for (const [songId, heard] of heardByGroup.get(groupName) ?? []) {
      const masterSong = masterSongs[songId];
      if (!masterSong) {
        continue;
      }
      const eventsList = Object.values(heard.events_by_id).sort((left, right) =>
        (left.event_date ?? "9999-99-99").localeCompare(right.event_date ?? "9999-99-99") ||
        left.event_source_id.localeCompare(right.event_source_id),
      );
      heardSongIdsGlobal.add(songId);
      heardSongs.push({
        source_song_id: songId,
        title: heard.title,
        appearances: heard.appearances,
        event_count: eventsList.length,
        first_heard_at: eventsList[0]?.event_date ?? null,
        first_heard_event: eventsList[0] ?? null,
        events: eventsList,
        master: masterSong,
      });
    }
    heardSongs.sort((left, right) => (left.first_heard_at ?? "9999-99-99").localeCompare(right.first_heard_at ?? "9999-99-99") || left.title.localeCompare(right.title));
    const heardIds = new Set(heardSongs.map((song) => song.source_song_id));
    const unheardSongs = Object.values(masterSongs)
      .filter((song) => !heardIds.has(song.source_song_id))
      .sort((left, right) => (left.first_released_at ?? "9999-99-99").localeCompare(right.first_released_at ?? "9999-99-99") || left.title.localeCompare(right.title));
    totalSongs += groupData.songs.length;
    groups[groupName] = {
      total_count: groupData.songs.length,
      heard_count: heardSongs.length,
      unheard_count: groupData.songs.length - heardSongs.length,
      coverage_percent: groupData.songs.length ? Math.round((heardSongs.length / groupData.songs.length) * 10000) / 100 : 0,
      heard_songs: heardSongs,
      unheard_songs: unheardSongs,
    };
  }

  return {
    summary: {
      input_event_ids: eventIds,
      input_event_count: eventIds.length,
      known_event_count: inputEvents.length,
      matched_event_count: inputEvents.filter((event) => event.matched_setlists.length > 0).length,
      matched_setlist_count: matchedSetlistKeys.size,
      unique_heard_song_count: heardSongIdsGlobal.size,
      total_group_song_count: totalSongs,
    },
    groups,
    input_events: inputEvents,
    issues: {
      unknown_event_ids: unknownEventIds,
      events_without_match: eventsWithoutMatch,
      missing_setlists: missingSetlists,
      unassigned_songs: unassignedSongs,
    },
  };
}

type HeardSong = {
  source_song_id: string;
  title: string;
  appearances: number;
  events_by_id: Record<
    string,
    EventSummary & {
      setlists: Array<{ source: string; source_id: string; title: string; url: string }>;
    }
  >;
};

function songMasterById(groupIndex: GroupSongIndex) {
  const songs: Record<string, { source_song_id: string; title: string; groups: string[]; master_by_group: Record<string, GroupSong> }> = {};
  for (const [groupName, groupData] of Object.entries(groupIndex.groups)) {
    for (const song of groupData.songs) {
      songs[song.source_song_id] ??= {
        source_song_id: song.source_song_id,
        title: song.title,
        groups: [],
        master_by_group: {},
      };
      songs[song.source_song_id].groups.push(groupName);
      songs[song.source_song_id].master_by_group[groupName] = song;
    }
  }
  return songs;
}

async function loadDataStore(): Promise<DataStore> {
  dataStorePromise ??= Promise.all([
    readJsonl<EventRecord>(path.join(DATA_ROOT, "raw/eventernote_main_groups_events.jsonl")),
    readJsonl<MatchRecord>(path.join(DATA_ROOT, "processed/main_groups_event_setlist_matches.jsonl")),
    readJsonl<SetlistRecord>(path.join(DATA_ROOT, "raw/llfans_all_setlists.jsonl")),
    readJson<GroupSongIndex>(path.join(DATA_ROOT, "processed/group_song_index.json")),
  ]).then(([events, matches, setlists, groupIndex]) => ({ events, matches, setlists, groupIndex }));
  return dataStorePromise;
}

async function readJson<T>(filePath: string): Promise<T> {
  return JSON.parse(await fs.readFile(filePath, "utf-8")) as T;
}

async function readJsonl<T>(filePath: string): Promise<T[]> {
  const content = await fs.readFile(filePath, "utf-8");
  return content
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => JSON.parse(line) as T);
}

function mainGroupNamesForEvent(event: EventRecord) {
  const groups = [];
  const performers = new Set(event.performers ?? []);
  const keywords = new Set(event.keywords ?? []);
  for (const [groupName, actorUrl] of Object.entries(MAIN_GROUP_ACTOR_URLS)) {
    if (keywords.has(actorUrl) || performers.has(groupName) || event.title.includes(groupName)) {
      groups.push(groupName);
    }
  }
  return groups;
}

function eventSummary(event: EventRecord): EventSummary {
  return {
    event_source_id: event.source_id,
    title: event.title,
    url: event.url,
    event_date: event.event_date,
    start_time: event.start_time,
    venue: event.venue,
  };
}

function normalizeHandle(handle: string) {
  const trimmed = handle.trim();
  if (!trimmed) {
    throw new Error("Eventernote handle is required");
  }
  if (!trimmed.startsWith("http://") && !trimmed.startsWith("https://")) {
    return trimmed.replace(/^@/, "").replace(/^\/+|\/+$/g, "");
  }
  const parsed = new URL(trimmed);
  const parts = parsed.pathname.split("/").filter(Boolean);
  if (parts[0] !== "users" || !parts[1]) {
    throw new Error("Expected an Eventernote user URL");
  }
  return decodeURIComponent(parts[1]);
}

function cleanText(value: string) {
  return value.replace(/\s+/g, " ").trim();
}

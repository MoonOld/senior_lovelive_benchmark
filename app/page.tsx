import { promises as fs } from "node:fs";
import path from "node:path";

export const dynamic = "force-static";

type CoverageSummary = {
  handle?: string;
  input_event_count: number;
  known_event_count: number;
  matched_event_count: number;
  matched_setlist_count: number;
  unique_heard_song_count: number;
  total_group_song_count?: number;
  user_event_count?: number;
  matched_lovelive_event_count?: number;
  unmatched_user_event_count?: number;
};

type Song = {
  source_song_id: string;
  title: string;
  appearances?: number;
  event_count?: number;
  first_heard_at?: string | null;
};

type GroupCoverage = {
  total_count: number;
  heard_count: number;
  unheard_count: number;
  coverage_percent: number;
  heard_songs: Song[];
  unheard_songs: Song[];
};

type InputEvent = {
  event_source_id: string;
  title: string;
  url: string;
  event_date?: string | null;
  start_time?: string | null;
  matched_setlists?: unknown[];
  heard_song_count?: number;
};

type CoverageAnalysis = {
  summary: CoverageSummary;
  groups: Record<string, GroupCoverage>;
  input_events: InputEvent[];
  issues?: {
    unknown_event_ids?: string[];
    events_without_match?: InputEvent[];
    missing_setlists?: unknown[];
    unassigned_songs?: unknown[];
  };
};

const COVERAGE_PATH = path.join(process.cwd(), "data/processed/handle_song_coverage_analysis.json");

async function loadCoverage(): Promise<CoverageAnalysis | null> {
  try {
    const content = await fs.readFile(COVERAGE_PATH, "utf-8");
    return JSON.parse(content) as CoverageAnalysis;
  } catch {
    return null;
  }
}

function formatPercent(value: number) {
  return `${value.toFixed(2)}%`;
}

function formatDate(event: InputEvent) {
  return [event.event_date ?? "unknown", event.start_time].filter(Boolean).join(" ");
}

export default async function Home() {
  const coverage = await loadCoverage();

  if (!coverage) {
    return (
      <main className="page">
        <section className="panel empty">
          <p className="eyebrow">Senior LoveLive Benchmark</p>
          <h1>Coverage data is not available</h1>
          <p className="lead">
            Vercel build is ready, but no deployable coverage snapshot was found at{" "}
            <code>data/processed/handle_song_coverage_analysis.json</code>. Run the handle coverage workflow and commit
            that generated JSON file to render the dashboard.
          </p>
        </section>
      </main>
    );
  }

  const { summary } = coverage;
  const groups = Object.entries(coverage.groups).sort(
    ([, left], [, right]) => right.coverage_percent - left.coverage_percent,
  );
  const issueCount =
    (coverage.issues?.unknown_event_ids?.length ?? 0) +
    (coverage.issues?.events_without_match?.length ?? 0) +
    (coverage.issues?.missing_setlists?.length ?? 0) +
    (coverage.issues?.unassigned_songs?.length ?? 0);

  return (
    <main className="page">
      <section className="hero">
        <div>
          <p className="eyebrow">Senior LoveLive Benchmark</p>
          <h1>{summary.handle ? `${summary.handle} 的歌曲覆盖率` : "LoveLive 歌曲覆盖率"}</h1>
          <p className="lead">
            基于 Eventernote handle 抓取结果、本地 LoveLive 活动库、LL-Fans setlist 与 discography 索引生成的静态覆盖率报告。
          </p>
        </div>
        <div className="panel">
          <div className="metric-label">Unique Heard Songs</div>
          <div className="metric-value">{summary.unique_heard_song_count}</div>
        </div>
      </section>

      <section className="summary-grid">
        <Metric label="User Events" value={summary.user_event_count ?? summary.input_event_count} />
        <Metric label="LoveLive Events" value={summary.matched_lovelive_event_count ?? summary.known_event_count} />
        <Metric label="Matched Setlists" value={summary.matched_setlist_count} />
        <Metric label="Issues" value={issueCount} />
      </section>

      <section className="section">
        <h2>Group Coverage</h2>
        <div className="group-grid">
          {groups.map(([groupName, group]) => (
            <article className="group-card" key={groupName}>
              <div className="group-card-header">
                <div>
                  <h3>{groupName}</h3>
                  <p className="muted">
                    {group.heard_count} / {group.total_count} heard, {group.unheard_count} unseen
                  </p>
                </div>
                <div className="coverage">{formatPercent(group.coverage_percent)}</div>
              </div>
              <div className="progress" aria-label={`${groupName} coverage`}>
                <div className="progress-bar" style={{ width: `${Math.min(group.coverage_percent, 100)}%` }} />
              </div>
              <div className="panel">
                <div className="metric-label">Recent heard songs</div>
                <ul className="song-list">
                  {group.heard_songs.slice(0, 5).map((song) => (
                    <li key={song.source_song_id}>
                      <strong>{song.title}</strong>
                      <div className="muted">
                        #{song.source_song_id}
                        {song.first_heard_at ? ` · first ${song.first_heard_at}` : ""}
                        {song.event_count ? ` · ${song.event_count} events` : ""}
                      </div>
                    </li>
                  ))}
                  {group.heard_songs.length === 0 ? <li className="muted">No heard songs yet.</li> : null}
                </ul>
              </div>
            </article>
          ))}
        </div>
      </section>

      <section className="section panel">
        <h2>Matched Events</h2>
        <ul className="event-list">
          {coverage.input_events.map((event) => (
            <li key={event.event_source_id}>
              <a href={event.url} rel="noreferrer" target="_blank">
                <strong>{event.title}</strong>
              </a>
              <div className="muted">
                {formatDate(event)} · #{event.event_source_id} · setlists={event.matched_setlists?.length ?? 0} · songs=
                {event.heard_song_count ?? 0}
              </div>
            </li>
          ))}
        </ul>
      </section>
    </main>
  );
}

function Metric({ label, value }: { label: string; value: number | string }) {
  return (
    <div className="metric">
      <div className="metric-label">{label}</div>
      <div className="metric-value">{value}</div>
    </div>
  );
}

"use client";

import { useEffect, useMemo, useState } from "react";
import type { CSSProperties, FormEvent, KeyboardEvent } from "react";

type CoverageSummary = {
  handle?: string;
  user_event_count?: number;
  user_event_page_count?: number;
  matched_lovelive_event_count?: number;
  unmatched_user_event_count?: number;
  matched_event_count: number;
  matched_setlist_count: number;
  unique_heard_song_count: number;
};

type Song = {
  source_song_id: string;
  title: string;
  appearances?: number;
  event_count?: number;
  first_heard_at?: string | null;
  first_released_at?: string | null;
  discographies?: Array<{ title?: string; released_at?: string | null }>;
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
  generated_at?: string;
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

type ApiError = {
  error: string;
};

const DEMO_HANDLE = "Tetsuya_Ryusei";
const GROUP_THEMES: Record<string, { color: string; soft: string; deep: string }> = {
  Aqours: { color: "#1597e5", soft: "rgba(21, 151, 229, 0.13)", deep: "#0867a3" },
  "μ’s": { color: "#f05ca8", soft: "rgba(240, 92, 168, 0.14)", deep: "#b52d77" },
  虹ヶ咲学園スクールアイドル同好会: { color: "#f59e0b", soft: "rgba(245, 158, 11, 0.15)", deep: "#ad6500" },
  "Liella!": { color: "#7c5cff", soft: "rgba(124, 92, 255, 0.13)", deep: "#4d35c6" },
  蓮ノ空女学院スクールアイドルクラブ: { color: "#10b981", soft: "rgba(16, 185, 129, 0.13)", deep: "#087a57" },
  "いきづらい部！": { color: "#ef4444", soft: "rgba(239, 68, 68, 0.12)", deep: "#b91c1c" },
  幻日のヨハネ: { color: "#4f46e5", soft: "rgba(79, 70, 229, 0.13)", deep: "#312e81" },
  スクールアイドルミュージカル: { color: "#14b8a6", soft: "rgba(20, 184, 166, 0.13)", deep: "#0f766e" },
};
const DEFAULT_GROUP_THEME = { color: "#ff5c9a", soft: "rgba(255, 92, 154, 0.13)", deep: "#bd2c62" };

export function CoverageExplorer() {
  const [handle, setHandle] = useState("");
  const [coverage, setCoverage] = useState<CoverageAnalysis | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(false);

  async function submit(event?: FormEvent<HTMLFormElement>, nextHandle = handle) {
    event?.preventDefault();
    const normalizedHandle = nextHandle.trim();
    if (!normalizedHandle) {
      setError("请输入 Eventernote 用户名。");
      return;
    }
    setIsLoading(true);
    setError(null);
    setCoverage(null);
    try {
      const response = await fetch(`/api/coverage?handle=${encodeURIComponent(normalizedHandle)}`);
      const data = (await response.json()) as CoverageAnalysis | ApiError;
      if (!response.ok) {
        throw new Error("error" in data ? data.error : "统计失败，请稍后重试。");
      }
      setCoverage(data as CoverageAnalysis);
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "统计失败，请稍后重试。");
    } finally {
      setIsLoading(false);
    }
  }

  function tryDemo() {
    setHandle(DEMO_HANDLE);
    void submit(undefined, DEMO_HANDLE);
  }

  return (
    <main className="app-shell">
      <section className="hero-card">
        <div className="hero-copy">
          <p className="eyebrow">LoveLive! Live Song Coverage</p>
          <h1>现场听歌覆盖率统计</h1>
          <p className="lead">
            输入 Eventernote 用户名，自动读取参加活动，并与 LoveLive 活动库、LL-Fans setlist、唱片曲库进行匹配。
          </p>
        </div>

        <form className="search-card" onSubmit={submit}>
          <label htmlFor="handle">
            <span>Eventernote 用户名</span>
            <a href="https://www.eventernote.com/" target="_blank" rel="noreferrer">
              eventernote.com
            </a>
          </label>
          <div className="search-row">
            <input
              id="handle"
              autoCapitalize="none"
              autoComplete="username"
              autoCorrect="off"
              onChange={(event) => setHandle(event.target.value)}
              placeholder={`例如：${DEMO_HANDLE}`}
              value={handle}
            />
            <button disabled={isLoading} type="submit">
              {isLoading ? "统计中..." : "开始统计"}
            </button>
          </div>
          <div className="helper-row">
            <button className="ghost-button" disabled={isLoading} onClick={tryDemo} type="button">
              试用 {DEMO_HANDLE}
            </button>
            <span>接口：GET /api/coverage?handle=&lt;username&gt;</span>
            <span>本站不会保存查询的用户名或统计结果</span>
          </div>
          {error ? <p className="form-error">{error}</p> : null}
        </form>
      </section>

      {isLoading ? <LoadingState /> : null}
      {coverage ? <CoverageResult coverage={coverage} /> : null}
    </main>
  );
}

function CoverageResult({ coverage }: { coverage: CoverageAnalysis }) {
  const { summary } = coverage;
  const groups = useMemo(
    () => Object.entries(coverage.groups).sort(([, left], [, right]) => right.coverage_percent - left.coverage_percent),
    [coverage.groups],
  );
  const [selectedGroupName, setSelectedGroupName] = useState(groups[0]?.[0] ?? "");
  const selectedGroup = selectedGroupName ? coverage.groups[selectedGroupName] : undefined;

  useEffect(() => {
    if (!groups.some(([groupName]) => groupName === selectedGroupName)) {
      setSelectedGroupName(groups[0]?.[0] ?? "");
    }
  }, [groups, selectedGroupName]);

  return (
    <section className="result-stack">
      <div className="result-header">
        <div>
          <p className="eyebrow">Result</p>
          <h2>{summary.handle} 的 LoveLive 覆盖率</h2>
        </div>
        {coverage.generated_at ? <span className="timestamp">生成于 {new Date(coverage.generated_at).toLocaleString()}</span> : null}
      </div>

      <div className="stats-grid">
        <Metric label="Eventernote 活动" value={summary.user_event_count ?? 0} />
        <Metric label="LoveLive 活动" value={summary.matched_lovelive_event_count ?? 0} />
        <Metric label="听过歌曲" value={summary.unique_heard_song_count} />
      </div>

      <div className="group-grid">
        {groups.map(([groupName, group]) => (
          <article
            aria-label={`${groupName} coverage detail`}
            aria-pressed={selectedGroupName === groupName}
            className={`group-card ${selectedGroupName === groupName ? "is-selected" : ""}`}
            key={groupName}
            onClick={() => setSelectedGroupName(groupName)}
            onKeyDown={(event) => handleGroupCardKeyDown(event, () => setSelectedGroupName(groupName))}
            role="button"
            style={groupThemeStyle(groupName)}
            tabIndex={0}
          >
            <div className="group-topline">
              <div>
                <h3>{groupName}</h3>
                <p>
                  {group.heard_count} / {group.total_count} 首已覆盖
                </p>
              </div>
              <strong>{group.coverage_percent.toFixed(2)}%</strong>
            </div>
            <div className="progress">
              <div style={{ width: `${Math.min(group.coverage_percent, 100)}%` }} />
            </div>
            <ul className="mini-list">
              {group.heard_songs.slice(0, 4).map((song) => (
                <li key={song.source_song_id}>
                  <span>{song.title}</span>
                  <small>{song.first_heard_at ?? "unknown"}</small>
                </li>
              ))}
              {group.heard_songs.length === 0 ? (
                <li>
                  <span>还没有匹配到歌曲</span>
                  <small>0 songs</small>
                </li>
              ) : null}
            </ul>
            <div className="card-action">查看歌曲明细</div>
          </article>
        ))}
      </div>

      {selectedGroup ? (
        <GroupSongDetail group={selectedGroup} groupName={selectedGroupName} style={groupThemeStyle(selectedGroupName)} />
      ) : null}

      <section className="events-panel">
        <div className="section-title">
          <h2>匹配到的活动</h2>
          <span>{coverage.input_events.length} events</span>
        </div>
        <ul className="event-list">
          {coverage.input_events.map((event) => (
            <li key={event.event_source_id}>
              <a href={event.url} rel="noreferrer" target="_blank">
                {event.title}
              </a>
              <span>
                {[event.event_date ?? "unknown", event.start_time].filter(Boolean).join(" ")} · songs=
                {event.heard_song_count ?? 0}
              </span>
            </li>
          ))}
        </ul>
      </section>
    </section>
  );
}

function GroupSongDetail({ group, groupName, style }: { group: GroupCoverage; groupName: string; style: CSSProperties }) {
  return (
    <section className="song-detail-panel" style={style}>
      <div className="section-title detail-title">
        <div>
          <p className="eyebrow">Song Detail</p>
          <h2>{groupName}</h2>
        </div>
        <span>
          已覆盖 {group.heard_count} / 未覆盖 {group.unheard_count}
        </span>
      </div>

      <div className="song-status-grid">
        <div className="song-status-column covered">
          <div className="song-status-heading">
            <span className="status-dot" />
            <h3>已覆盖</h3>
            <strong>{group.heard_count}</strong>
          </div>
          <ul className="song-detail-list">
            {group.heard_songs.map((song) => (
              <li className="song-detail-item is-covered" key={song.source_song_id}>
                <div>
                  <strong>{song.title}</strong>
                  <p>
                    首次听到：{song.first_heard_at ?? "unknown"} · 活动 {song.event_count ?? 0} 场 · 出现{" "}
                    {song.appearances ?? 0} 次
                  </p>
                </div>
                <span className="song-badge covered">covered</span>
              </li>
            ))}
            {group.heard_songs.length === 0 ? <EmptySongItem label="还没有覆盖歌曲" /> : null}
          </ul>
        </div>

        <div className="song-status-column uncovered">
          <div className="song-status-heading">
            <span className="status-dot" />
            <h3>未覆盖</h3>
            <strong>{group.unheard_count}</strong>
          </div>
          <ul className="song-detail-list">
            {group.unheard_songs.map((song) => {
              const source = song.discographies?.[0]?.title;
              return (
                <li className="song-detail-item is-uncovered" key={song.source_song_id}>
                  <div>
                    <strong>{song.title}</strong>
                    <p>
                      {song.first_released_at ? `发行：${song.first_released_at}` : "发行日 unknown"}
                      {source ? ` · ${source}` : ""}
                    </p>
                  </div>
                  <span className="song-badge uncovered">missing</span>
                </li>
              );
            })}
            {group.unheard_songs.length === 0 ? <EmptySongItem label="这个团体已经全部覆盖" /> : null}
          </ul>
        </div>
      </div>
    </section>
  );
}

function EmptySongItem({ label }: { label: string }) {
  return (
    <li className="song-detail-item empty-song">
      <div>
        <strong>{label}</strong>
        <p>换一个团体或补充更多活动后再查看。</p>
      </div>
    </li>
  );
}

function LoadingState() {
  return (
    <section className="loading-card">
      <div className="loader" />
      <div>
        <h2>正在统计</h2>
        <p>正在访问 Eventernote 并计算歌曲覆盖率，通常需要几秒钟。</p>
      </div>
    </section>
  );
}

function handleGroupCardKeyDown(event: KeyboardEvent<HTMLElement>, selectGroup: () => void) {
  if (event.key !== "Enter" && event.key !== " ") {
    return;
  }
  event.preventDefault();
  selectGroup();
}

function Metric({ label, value }: { label: string; value: number | string }) {
  return (
    <div className="metric">
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function groupThemeStyle(groupName: string): CSSProperties {
  const theme = GROUP_THEMES[groupName] ?? DEFAULT_GROUP_THEME;
  return {
    "--group-color": theme.color,
    "--group-soft": theme.soft,
    "--group-deep": theme.deep,
  } as CSSProperties;
}

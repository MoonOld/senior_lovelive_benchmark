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

type SeniorityVerdict = {
  level: "legend" | "veteran" | "regular" | "rookie";
  title: string;
  description: string;
  reasons: string[];
};

const DEMO_HANDLE = "Tetsuya_Ryusei";
const GROUP_THEMES: Record<string, { color: string; soft: string; deep: string }> = {
  Aqours: { color: "#42b8dd", soft: "rgba(66, 184, 221, 0.14)", deep: "#137ea2" },
  "μ’s": { color: "#e64aa3", soft: "rgba(230, 74, 163, 0.14)", deep: "#b42378" },
  虹ヶ咲学園スクールアイドル同好会: { color: "#e6c84d", soft: "rgba(230, 200, 77, 0.18)", deep: "#a67b00" },
  "Liella!": { color: "#c65abd", soft: "rgba(198, 90, 189, 0.14)", deep: "#8e2f86" },
  蓮ノ空女学院スクールアイドルクラブ: { color: "#d98aa5", soft: "rgba(217, 138, 165, 0.16)", deep: "#a64f6b" },
  "いきづらい部！": { color: "#b98b52", soft: "rgba(185, 139, 82, 0.15)", deep: "#7b5426" },
  幻日のヨハネ: { color: "#4aa7d8", soft: "rgba(74, 167, 216, 0.14)", deep: "#1b6f9a" },
  スクールアイドルミュージカル: { color: "#caa94a", soft: "rgba(202, 169, 74, 0.16)", deep: "#8d6b16" },
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
      setError("先交出 Eventernote 用户名，老资历鉴定才开工。");
      return;
    }
    setIsLoading(true);
    setError(null);
    setCoverage(null);
    try {
      const response = await fetch(`/api/coverage?handle=${encodeURIComponent(normalizedHandle)}`);
      const data = (await response.json()) as CoverageAnalysis | ApiError;
      if (!response.ok) {
        throw new Error("error" in data ? data.error : "鉴定失败了，可能是 Eventernote 今天也在摆烂。稍后再试。");
      }
      setCoverage(data as CoverageAnalysis);
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "鉴定失败了，可能是 Eventernote 今天也在摆烂。稍后再试。");
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
          <p className="eyebrow">老资历审查委员会</p>
          <h1>LoveLive 老资历程度鉴定</h1>
          <p className="lead">
            输入 Eventernote 用户名，看看你到底只是路过沼津，还是已经在会场地板上长出了年轮。
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
              {isLoading ? "翻牌中..." : "开始鉴定"}
            </button>
          </div>
          <div className="helper-row">
            <button className="ghost-button" disabled={isLoading} onClick={tryDemo} type="button">
              试用 {DEMO_HANDLE}
            </button>
            <span>本站不留案底，不保存查询的用户名或统计结果</span>
          </div>
          {error ? <p className="form-error">{error}</p> : null}
        </form>
      </section>

      {isLoading ? <LoadingState /> : null}
      {coverage ? <CoverageResult coverage={coverage} /> : null}
      <footer className="site-footer">
        感谢{" "}
        <a href="https://ll-fans.jp/" rel="noreferrer" target="_blank">
          LL-Fans
        </a>{" "}
        的数据支持
      </footer>
    </main>
  );
}

function CoverageResult({ coverage }: { coverage: CoverageAnalysis }) {
  const { summary } = coverage;
  const groups = useMemo(
    () => Object.entries(coverage.groups).sort(([, left], [, right]) => right.coverage_percent - left.coverage_percent),
    [coverage.groups],
  );
  const verdict = useMemo(() => buildSeniorityVerdict(summary, coverage.groups), [coverage.groups, summary]);
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
          <p className="eyebrow">鉴定书已出</p>
          <h2>{summary.handle} 的老资历鉴定书</h2>
        </div>
        {coverage.generated_at ? <span className="timestamp">生成于 {new Date(coverage.generated_at).toLocaleString()}</span> : null}
      </div>

      <SeniorityCard verdict={verdict} />

      <div className="stats-grid">
        <Metric label="翻到的活动" value={summary.user_event_count ?? 0} />
        <Metric label="命中的拉拉现场" value={summary.matched_lovelive_event_count ?? 0} />
        <Metric label="现场听过的歌" value={summary.unique_heard_song_count} />
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
                  已解锁 {group.heard_count} / {group.total_count} 首
                </p>
              </div>
              <strong>{group.coverage_percent.toFixed(2)}%</strong>
            </div>
            <div className="progress">
              <div style={{ width: `${Math.min(group.coverage_percent, 100)}%` }} />
            </div>
            {group.heard_songs.length > 0 ? (
              <ul className="mini-list">
                {group.heard_songs.slice(0, 4).map((song) => (
                  <li key={song.source_song_id}>
                    <span title={song.title}>{song.title}</span>
                    <small>{song.first_heard_at ?? "unknown"}</small>
                  </li>
                ))}
              </ul>
            ) : (
              <div className="card-empty">小资历 ylg 养成中，下一场就来点亮这里</div>
            )}
            <div className="card-action">展开资历明细</div>
          </article>
        ))}
      </div>

      {selectedGroup ? (
        <GroupSongDetail group={selectedGroup} groupName={selectedGroupName} style={groupThemeStyle(selectedGroupName)} />
      ) : null}

      <section className="events-panel">
        <div className="section-title">
          <h2>被翻出来的现场</h2>
          <span>{coverage.input_events.length} events</span>
        </div>
        <ul className="event-list">
          {coverage.input_events.map((event) => (
            <li key={event.event_source_id}>
              <a href={event.url} rel="noreferrer" target="_blank">
                {event.title}
              </a>
              <span>
                {[event.event_date ?? "unknown", event.start_time].filter(Boolean).join(" ")} · 现场歌曲=
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
          <p className="eyebrow">资历明细</p>
          <h2>{groupName}</h2>
        </div>
        <span>
          现场听过 {group.heard_count} / 还没逮到 {group.unheard_count}
        </span>
      </div>

      <div className="song-status-grid">
        <div className="song-status-column covered">
          <div className="song-status-heading">
            <span className="status-dot" />
            <h3>已经在现场听过</h3>
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
                <span className="song-badge covered">听过</span>
              </li>
            ))}
            {group.heard_songs.length === 0 ? <EmptySongItem label="这栏还空着，资历正在加载中" /> : null}
          </ul>
        </div>

        <div className="song-status-column uncovered">
          <div className="song-status-heading">
            <span className="status-dot" />
            <h3>还没在现场逮到</h3>
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
                  <span className="song-badge uncovered">待逮捕</span>
                </li>
              );
            })}
            {group.unheard_songs.length === 0 ? <EmptySongItem label="这个团已经被你盘包浆了" /> : null}
          </ul>
        </div>
      </div>
    </section>
  );
}

function SeniorityCard({ verdict }: { verdict: SeniorityVerdict }) {
  return (
    <section className={`seniority-card ${verdict.level}`}>
      <div>
        <p className="eyebrow">老资历判定</p>
        <h3>{verdict.title}</h3>
        <p>{verdict.description}</p>
      </div>
      <ul>
        {verdict.reasons.map((reason) => (
          <li key={reason}>{reason}</li>
        ))}
      </ul>
    </section>
  );
}

function EmptySongItem({ label }: { label: string }) {
  return (
    <li className="song-detail-item empty-song">
      <div>
        <strong>{label}</strong>
        <p>换个团体看看，或者下一场继续攒资历。</p>
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
        <p>正在翻 Eventernote 小本本，资历越老，翻牌越久。</p>
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

function buildSeniorityVerdict(summary: CoverageSummary, groups: Record<string, GroupCoverage>): SeniorityVerdict {
  const liveEventCount = summary.matched_lovelive_event_count ?? summary.matched_event_count ?? 0;
  const heardSongCount = summary.unique_heard_song_count ?? 0;
  const topGroup = Object.entries(groups).reduce<{ name: string; coverage: GroupCoverage } | null>((best, [name, coverage]) => {
    if (!best || coverage.coverage_percent > best.coverage.coverage_percent) {
      return { name, coverage };
    }
    return best;
  }, null);
  const topGroupText = topGroup
    ? `${topGroup.name} 覆盖率 ${topGroup.coverage.coverage_percent.toFixed(2)}%`
    : "还没有团体覆盖率数据";
  const topCoverage = topGroup?.coverage.coverage_percent ?? 0;
  const reasons = [`命中的拉拉现场：${liveEventCount} 场`, topGroupText, `现场听过的歌：${heardSongCount} 首`];

  if (liveEventCount >= 20 || topCoverage >= 60) {
    return {
      level: "legend",
      title: "老资历，给你跪了",
      description: "这已经不是普通参加活动了，这是把时间线踩出包浆的程度。",
      reasons,
    };
  }
  if (liveEventCount >= 10 || topCoverage >= 35) {
    return {
      level: "veteran",
      title: "资历很硬，已经不是普通观众了",
      description: "你不是路过会场，你是在会场附近拥有精神房产。",
      reasons,
    };
  }
  if (liveEventCount >= 3 || topCoverage >= 15) {
    return {
      level: "regular",
      title: "入坑姿势很稳，年轮开始长了",
      description: "已经能看出明显活动轨迹，再多跑几场就要被叫前辈了。",
      reasons,
    };
  }
  return {
    level: "rookie",
    title: "资历刚起步，下一场就安排",
    description: "小本本还很清爽，说明未来还有大量名场面等你解锁。",
    reasons,
  };
}

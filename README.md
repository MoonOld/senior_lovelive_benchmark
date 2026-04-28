# Senior LoveLive Benchmark

LoveLive 相关演出与 setlist 数据采集工具。

项目第一版是一个 Python CLI：先从 Eventernote 抓取 LoveLive 相关活动，再以每个 Eventernote event 为入口，到 LiveFans 和 setlist.fm 查找对应 setlist，最后输出 JSONL 和 Markdown 关联结果。

## 数据源

- Eventernote：活动主数据源，按关键词抓取活动搜索结果和详情页。
- LiveFans：补充 setlist 数据，优先根据 Eventernote event 的标题、简化标题和出演团体定向搜索，再解析详情页曲目。
- setlist.fm：补充 setlist 数据，优先使用官方 API。

默认 Eventernote 关键词包括 `LoveLive`、`ラブライブ`、`Liella`、`Aqours`、`虹ヶ咲`、`蓮ノ空`、`μ's`。setlist 抓取采用 Eventernote-first 策略：例如给定 `LoveLive! Series 15th Anniversary ラブライブ！フェス Day.2`，会优先用这场 event 的标题和简化标题去搜索候选 setlist，而不是只做全局关键词粗搜。

## 环境要求

- Python 3.9+
- 网络访问 Eventernote、LiveFans、setlist.fm
- 如需使用 setlist.fm，需要 API key，并通过 `SETLISTFM_API_KEY` 环境变量或 CLI 参数传入。

## 安装

```bash
make install
```

查看命令：

```bash
make help
make cli-help
```

## 快速开始

小样本抓取：

```bash
make sample
```

等价于按默认变量执行 Eventernote 小样本抓取、基于 Eventernote events 的 LiveFans 候选抓取、匹配和 Markdown 导出。

全量抓取时可以把分页参数设为 `0`，表示持续跟随页面上明确存在的下一页链接：

```bash
make full
```

启用 setlist.fm：

```bash
export SETLISTFM_API_KEY="your-api-key"
make crawl-setlists-with-setlistfm
```

覆盖关键词、分页、请求间隔或匹配阈值：

```bash
make sample KEYWORD="LoveLive! Series Asia Tour 2024" SETLIST_KEYWORD="LoveLive" DELAY=1
make full THRESHOLD=65 TOP_N=3 LIVEFANS_EVENT_QUERIES=3
```

## Make 命令

### `make crawl-eventernote`

抓取 Eventernote 活动数据。

常用变量：

- `KEYWORD`：搜索关键词，默认 `LoveLive! Series Asia Tour 2024`。
- `EVENT_PAGES`：抓取页数；`0` 表示持续跟随下一页。
- `DELAY`：请求间隔秒数，默认 `1`。

示例：

```bash
make crawl-eventernote KEYWORD="ラブライブ" EVENT_PAGES=1 DELAY=1
```

### `make crawl-livefans`

只抓取 LiveFans setlist 数据，不调用 setlist.fm。默认会读取 `data/raw/eventernote_events.jsonl`，并为每个 Eventernote event 生成若干搜索查询；如果 Eventernote 数据不存在，才退回 `SETLIST_KEYWORD` 全局搜索。

常用变量：

- `SETLIST_KEYWORD`：LiveFans 搜索关键词，默认 `LoveLive`。
- `LIVEFANS_PAGES`：LiveFans 抓取页数；`0` 表示持续跟随下一页。
- `LIVEFANS_EVENT_QUERIES`：每个 Eventernote event 最多生成的 LiveFans 搜索查询数，默认 `3`。
- `DELAY`：请求间隔秒数，默认 `1`。

示例：

```bash
make crawl-livefans SETLIST_KEYWORD="ラブライブ" LIVEFANS_PAGES=1 LIVEFANS_EVENT_QUERIES=3 DELAY=1
```

### `make crawl-setlists-with-setlistfm`

抓取 LiveFans，并启用 setlist.fm。需要先设置 `SETLISTFM_API_KEY`。

```bash
export SETLISTFM_API_KEY="your-api-key"
make crawl-setlists-with-setlistfm SETLIST_KEYWORD="LoveLive" LIVEFANS_PAGES=1 LIVEFANS_EVENT_QUERIES=3 DELAY=1
```

### `make match`

把 Eventernote 活动和 setlist 候选做模糊匹配。

常用变量：

- `THRESHOLD`：匹配阈值，默认 `55`。
- `TOP_N`：每个活动最多保留的候选数量，默认 `2`。

示例：

```bash
make match THRESHOLD=65 TOP_N=3
```

匹配依据包括日期、标题、场馆、出演者和是否有曲目。跨日期不会匹配；缺失日期会降权。

### `make export`

把活动、setlist 和匹配结果导出成 Markdown，方便人工检查。

默认输出：

```text
data/exports/lovelive_event_setlists.md
```

### 其他命令

```bash
make install
make compile
make cli-help
make clean-data
```

## 数据产物

默认输出文件：

- `data/raw/eventernote_events.jsonl`：Eventernote 活动记录。
- `data/raw/livefans_setlists.jsonl`：LiveFans setlist 记录。
- `data/raw/setlistfm_setlists.jsonl`：setlist.fm setlist 记录。
- `data/processed/event_setlist_matches.jsonl`：活动与 setlist 匹配结果。
- `data/exports/lovelive_event_setlists.md`：人工浏览用汇总。

`data/raw/`、`data/processed/`、`data/exports/` 默认被 Git 忽略，避免把采集数据直接提交到仓库。

## 数据格式概览

`EventRecord` 主要字段：

- `source_id`
- `title`
- `url`
- `event_date`
- `open_time`
- `start_time`
- `end_time`
- `venue`
- `performers`
- `related_links`
- `keywords`
- `attendee_count`

`SetlistRecord` 主要字段：

- `source`
- `source_id`
- `title`
- `url`
- `event_date`
- `venue`
- `artists`
- `songs`

`MatchRecord` 主要字段：

- `event_source_id`
- `setlist_source`
- `setlist_source_id`
- `match_score`
- `match_reason`
- `event_url`
- `setlist_url`
- `reviewed`

## 注意事项

- 请使用较低频率抓取，建议 `DELAY=1` 或更高。
- setlist.fm 有 API 限流，批量抓取时应控制页数和请求量。
- Eventernote 和 LiveFans 页面结构可能变化，采集结果需要抽样人工核对。
- `match_score` 是辅助判断，不代表绝对正确；低置信度或直播/转播活动尤其需要人工复核。
- 不要提交 `.env` 或 API key，项目已在 `.gitignore` 中忽略 `.env` 和 `.env.*`。

## 当前状态

已实现：

- Eventernote 活动搜索与详情解析。
- LiveFans setlist 搜索与详情解析。
- setlist.fm API 客户端。
- JSONL 合并、去重和原子写入。
- 活动与 setlist 的匹配评分。
- Markdown 导出。

尚未包含：

- 自动化测试。
- SQLite 存储。
- Web UI 或 API 服务。

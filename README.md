# news-collector

美股突发新闻采集器：GitHub Actions 每 5–10 分钟跑一次，机械预筛后把候选写进
`inbox.json`，供 Muse 的推送轮次（实时快讯 / 凌晨扫描 / 盘前简报 / 盘后汇总）读。

## 它做什么、不做什么

- 做：拉源、去重、窗口过滤、机械匹配（watchlist 代码/公司名、高信号关键词桶）。
- 不做：不判断市值、不判断 2 小时硬线、不判断实质性、不推送、不写链接结论。
  这些全部留给 Muse 的轮次。采集器只负责"别漏、别重、别把旧闻当新的"。

## 源（v2）

1. **Market Terminal** `https://yucanding-market-terminal.hf.space/api/news`：聚合 24 个源；
   最新条目超过 20 分钟就调一次 `/refresh` 再重读。
2. **富途** `ai-news-search.futunn.com/news_search`：15 只重点代码，中文新闻，`size=10`。
3. **Google News RSS**：`Ming-Chi Kuo`、`郭明錤`、并购洽谈、做空机构 4 个查询。
   链接是 Google 跳转链接（`link_is_redirect: true`），Muse 读到后须自行溯源到原始发布方。

v2（2026-11-21）：去掉 CMS 官网源——用户另有 CMS 定期监控（cms-news-monitor），本采集器不再重复查。

任何一个源挂了只记进 `errors`，不让整轮失败；源全挂时 inbox 保留窗口内旧条目并标
`stale: true`，Muse 轮次读到后回落到直接搜索。

## 文件

- `collector.py`：采集器本体，只用 Python 标准库，不装依赖。
- `watchlist.json`：代码 -> 公司名（中英文），标题/摘要命中即进 inbox。
- `inbox.json`：候选清单，12 小时窗口、最多 60 条、按发布时间倒序。
  每条含 `title / summary / link / source / published_utc / tickers / cats`。
- `seen.json`：去重状态（id -> 首次见到时间），保留 14 天。
- `.github/workflows/collect.yml`：排期与提交逻辑。

## 排期（UTC；美东 = UTC-5）

- 5 分钟：美东 03:00–11:00（北京时间 16:00–24:00，Muse 热档）
- 10 分钟：其余时段；周六美东 10:00–13:00 另加（喂周末轮次）
- 分钟：仓库公开时标准 runner 免费不限量。若改回私有：每周约 1,134 次运行，
  GitHub 按 job 向上取整到分钟、每次至少计 1 分钟，每月约 4,900 分钟，
  超免费档 2,000 分钟/月（账号下所有私有仓库共用一个池子）。

## Muse 轮次怎么用

轮次先读 `inbox.json`（`get_file_contents` 或 raw 链接），只对 inbox 里的候选做
判断和推送；inbox 为空或 `stale: true` 时回落到现在的直接搜索流程。
inbox 里的条目仍须逐条核实首发时间与原始链接，不得直接采用。

# news-collector

美股突发新闻采集器：GitHub Actions 每 5–10 分钟跑一次，机械预筛后把候选写进
`inbox.json`，供 Muse 的推送轮次（实时快讯 / 凌晨扫描 / 盘前简报 / 盘后汇总）读。

## 它做什么、不做什么

- 做：拉源、去重、窗口过滤、机械匹配（watchlist 代码/公司名、高信号关键词桶）。
- 不做：不判断市值、不判断 2 小时硬线、不判断实质性、不推送、不写链接结论。
  这些全部留给 Muse 的轮次。采集器只负责"别漏、别重、别把旧闻当新的"。

## 源（v1）

1. **Market Terminal** `https://yucanding-market-terminal.hf.space/api/news`：聚合 24 个源；
   最新条目超过 20 分钟就调一次 `/refresh` 再重读。
2. **富途** `ai-news-search.futunn.com/news_search`：20 只重点代码，中文新闻，`size=10`。
3. **Google News RSS**：`Ming-Chi Kuo`、`郭明錤`、并购洽谈、做空机构 4 个查询。
   链接是 Google 跳转链接（`link_is_redirect: true`），Muse 读到后须自行溯源到原始发布方。
4. **CMS 官网** `cms.gov/newsroom/articles`：只看第 0 页、只取美东今天/昨天，尽力而为。

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

- 5 分钟：美东 09:00–16:00（盘中忙时）
- 10 分钟：美东 04:00–09:00、16:00–21:00；周日 18:00 起
- 每月约 1,500–1,600 分钟（GitHub 免费档 2,000 分钟/月，私有仓库）。
  若超了：改公开仓库（分钟数不限）或把忙时改回 10 分钟。

## Muse 轮次怎么用

轮次先读 `inbox.json`（`get_file_contents` 或 raw 链接），只对 inbox 里的候选做
判断和推送；inbox 为空或 `stale: true` 时回落到现在的直接搜索流程。
inbox 里的条目仍须逐条核实首发时间与原始链接，不得直接采用。

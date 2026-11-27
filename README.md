# news-collector

美股突发新闻采集 + Gemini 判断 + Telegram 推送（GitHub Actions 版，v4.1）。

每 5–10 分钟跑一次：拉源 → 机械预筛 → 打分聚类 → Gemini 一次批量判断 → 代码二次闸门 → 符合突发快讯标准的立即推 Telegram（@premarketnews_bot）。只推低频、高信噪比、确实会马上动价格的新闻。

## 流水线

1. 拉源：每个源独立（单独记健康度）。一手源（SEC 8-K、SEC 新闻稿、PR Newswire）+ Market Terminal 聚合 + 富途 + Google News。
2. 代码硬过滤：时间戳缺失/超窗、律所广告/盘点/评论/财报/评级噪音、黑名单信源。
3. 打分 + 聚类：同一事件多源只送信源级别最高的一条。
4. Gemini 一次批量判断（结构化 JSON 输出，最多 15 条/轮）。
5. 代码二次闸门：重要度、方向、原文证据、代码合法性、去重、信源级别。
6. 推送：发送失败进 pending，下轮直接重发（不重判）；每推一条立即落盘。

## 推送标准（摘要）

- 时效：2 小时硬线（发布时间 120 分钟内，超了不推）。
- 规模：直接受影响的美股公司市值明显高于约 20 亿美元；唯一例外是单项约 10 亿美元以上的政府/军方合同或融资。
- 不推：财报与指引本身、分析师评级/目标价、律所广告、营销稿、观点/解读/回顾、旧事重提。
- 只推：并购、做空报告、监管/联邦机构动作、高管变动、重大合同、召回/事故、供应链实质变化、郭明錤爆料、此前催化剂被权威反转、官方硬数字销售纪录。
- 重要度 ≥ 4（1–5 分）才推；拿不准能不能马上动价格的一律降到 3，不推。单轮最多推 3 条。
- 链接必须是原始发布方（公司 IR、Business Wire / PR Newswire / GlobeNewswire 官宣、SEC EDGAR），不链综述/聚合页。

## 推送格式（统一五行）

```
🚨 新闻：英伟达与马来西亚签署 AI 数据中心合作协议，总投资 20 亿美元
🏢 公司：$NVDA
🧭 方向：利好 📈
🕐 时间：11-27 11:20 ET
🔗 来源：Reuters
```

- 方向：利好 📈，利空 📉（行首 🧭 利好利空不变，文末跟方向 emoji）。
- 时间：美东时间，后缀 ET。
- 来源：名字是可点击的链接，点一下跳原文。
- 有实质性进展时，新闻行开头带【进展】。
- Telegram 只收这种新闻推送，任何报错/告警都不发。

## 源

- Market Terminal：聚合 24 个源；最新条目超过 20 分钟就调一次 `/refresh` 再重读。
- 富途：15 只重点代码，中文新闻，`size=10`。
- SEC 8-K：只取 watchlist 公司的 8-K，且仅限高信号 Item（重大协议/高管变动/控制权变更等，不含财报），须配 `SEC_CONTACT_EMAIL`。
- SEC 新闻稿 RSS、PR Newswire（金融/科技/健康三个频道）。
- Google News RSS（带 `when:2h`）：郭明錤（中英）、顶级媒体独家、顶级媒体并购、BusinessWire、做空机构、供应链（DigiTimes / TrendForce / Nikkei）。

已移除的源：FDA、FTC 新闻稿 RSS 与 GlobeNewswire 并购 RSS——地址本身没错，但这三个站挡了 GitHub Actions 的机房 IP（固定 404/403），拉不到。FDA/FTC 的大新闻仍由 Terminal 聚合与媒体报导覆盖。

## 文件

- `collector.py`：采集器本体，只用 Python 标准库，不装依赖。
- `watchlist.json`：代码 -> 公司名（中英文），标题/摘要命中即进候选。
- `inbox.json`：候选清单，12 小时窗口、最多 800 条、按发布时间倒序。
- `seen.json`：去重状态（id -> 首次见到时间），保留 14 天。
- `pushed.json`：已推事件（72 小时）+ 待重发的 pending。
- `judged.json`：已判条目（12 小时）+ 信源级别 rank，出现更好信源时重判。
- `decisions.jsonl`：每次判断的审计日志（推/不推 + 原因，最多 1500 行）。
- `health.json`：各源失败次数与最近错误（只记文件，不发 Telegram）。
- `sec_cik.json`：SEC 公司代码缓存（7 天刷新一次）。
- `.github/workflows/collect.yml`：只留 workflow_dispatch，由 cron-job.org dispatch；提交逻辑见该文件。

## Secrets（仓库 Settings → Secrets）

| Secret | 必填 | 说明 |
|---|---|---|
| `GEMINI_API_KEY` | 是 | Gemini API key |
| `TELEGRAM_BOT_TOKEN` | 是 | 推送 bot 的 token |
| `SEC_CONTACT_EMAIL` | 否 | 联系邮箱（SEC 要求 UA 带邮箱）；不配则跳过 8-K 源与 SEC RSS |
| `GEMINI_MODEL` | 否 | 指定优先模型；不配则自动列出可用模型按偏好选 |
| `TELEGRAM_CHAT_ID` | 否 | 默认 6288855088 |
| `HEALTHCHECK_URL` | 否 | healthchecks.io 之类的心跳地址 |

## 触发（cron-job.org）

工作流只有 `workflow_dispatch`，不设 GitHub 原生 schedule（已证实不可靠）。由 cron-job.org 的四个 dispatch 任务触发：Hot（每 5 分钟）、Day / Night / Sat（每 10 分钟）。并发组 `news-collector`，`cancel-in-progress: true`。

## 模型

候选模型共 14 个（一个都不删），按偏好顺序回退，另加动态列出可用模型（新模型自动进入候选）：gemini-3.8-flash、3.7-flash、3.6-flash、3.5-flash、3.5-flash-lite、3-flash-preview、2.5-flash、2.5-flash-lite、2.0-flash、2.0-flash-lite、1.5-flash、1.5-flash-8b、1.5-pro、2.5-pro。可用 `GEMINI_MODEL` 环境变量指定优先模型。

## 本地测试

```bash
# 影子测试：只打印不发送、不写 judged/pushed
DRY_RUN=1 GEMINI_API_KEY=... TELEGRAM_BOT_TOKEN=... python3 collector.py
```

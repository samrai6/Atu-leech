import asyncio
import logging
import os
import sqlite3
import threading
import time
from pathlib import Path

from flask import Flask, jsonify
from telethon import TelegramClient, events
from telethon.errors import FloodWaitError
from telethon.sessions import StringSession

# -------------------- Configuration --------------------
API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
SESSION_STRING = os.environ["SESSION_STRING"]

SOURCE_CHANNEL_ID = int(os.getenv("SOURCE_CHANNEL_ID", "-1002548576007"))
LEECH_GROUP_ID = -5570627643

# Each command is sent as a reply to the forwarded source message.
BOTS = [
    {"name": "Bot 1", "username": os.getenv("BOT1_USERNAME", "SMDxLMcJeniBot"), "command": os.getenv("BOT1_COMMAND", "/1")},
    {"name": "Bot 2", "username": os.getenv("BOT2_USERNAME", "SMDxLmjenibot"), "command": os.getenv("BOT2_COMMAND", "/l2")},
    {"name": "Bot 3", "username": os.getenv("BOT3_USERNAME", "AfLeechxBot"), "command": os.getenv("BOT3_COMMAND", "/l3")},
]

LIMIT_PER_BOT = int(os.getenv("LIMIT_PER_BOT", "3"))
WINDOW_SECONDS = int(os.getenv("WINDOW_SECONDS", "900"))
DB_PATH = os.getenv("DB_PATH", "autoleech.sqlite3")
PORT = int(os.getenv("PORT", "10000"))

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("atu-leech")

app = Flask(__name__)
client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
worker_task = None


# -------------------- Health endpoint --------------------
@app.get("/")
@app.get("/health")
def health():
    return jsonify({"status": "ok", "service": "Atu-leech", "telegram_connected": bool(client.is_connected())})


def start_health_server():
    app.run(host="0.0.0.0", port=PORT, use_reloader=False)


# -------------------- SQLite persistence --------------------
def db_connect():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    with db_connect() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_chat_id INTEGER NOT NULL,
                source_message_id INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'queued',
                bot_index INTEGER,
                forwarded_message_id INTEGER,
                attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                UNIQUE(source_chat_id, source_message_id)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS bot_usage (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bot_index INTEGER NOT NULL,
                used_at REAL NOT NULL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_status_id ON jobs(status, id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_bot_usage_window ON bot_usage(bot_index, used_at)")


def enqueue_message(chat_id: int, message_id: int):
    now = time.time()
    with db_connect() as conn:
        cur = conn.execute(
            """INSERT OR IGNORE INTO jobs
               (source_chat_id, source_message_id, status, created_at, updated_at)
               VALUES (?, ?, 'queued', ?, ?)""",
            (chat_id, message_id, now, now),
        )
        return cur.rowcount == 1


def get_next_job():
    with db_connect() as conn:
        row = conn.execute(
            "SELECT * FROM jobs WHERE status IN ('queued', 'forwarded') ORDER BY id LIMIT 1"
        ).fetchone()
        return dict(row) if row else None


def update_job(job_id: int, **fields):
    if not fields:
        return
    fields["updated_at"] = time.time()
    columns = ", ".join(f"{key} = ?" for key in fields)
    values = list(fields.values()) + [job_id]
    with db_connect() as conn:
        conn.execute(f"UPDATE jobs SET {columns} WHERE id = ?", values)


def select_bot_for_job(job_id: int) -> int:
    # Fixed batches: 3 files to Bot 1, next 3 to Bot 2, next 3 to Bot 3, then repeat.
    return ((job_id - 1) // 3) % len(BOTS)


def seconds_until_bot_available(bot_index: int) -> float:
    now = time.time()
    cutoff = now - WINDOW_SECONDS
    with db_connect() as conn:
        row = conn.execute(
            """SELECT COUNT(*) AS n, MIN(used_at) AS oldest
               FROM bot_usage WHERE bot_index = ? AND used_at > ?""",
            (bot_index, cutoff),
        ).fetchone()

    if row["n"] < LIMIT_PER_BOT or row["oldest"] is None:
        return 0.0
    return max(0.0, float(row["oldest"]) + WINDOW_SECONDS - now)


def record_bot_usage(bot_index: int):
    with db_connect() as conn:
        conn.execute(
            "INSERT INTO bot_usage(bot_index, used_at) VALUES (?, ?)",
            (bot_index, time.time()),
        )


async def get_source_message(job):
    try:
        return await client.get_messages(job["source_chat_id"], ids=job["source_message_id"])
    except Exception:
        log.exception("Could not retrieve source message %s", job["source_message_id"])
        return None


async def process_job(job):
    job_id = job["id"]
    bot_index = select_bot_for_job(job_id)
    bot = BOTS[bot_index]
    update_job(job_id, bot_index=bot_index, attempts=job["attempts"] + 1)

    # Keep queue order. If the assigned bot is at its 3-per-15-minute limit,
    # wait for its oldest slot to expire instead of skipping/reordering files.
    while True:
        wait_for = seconds_until_bot_available(bot_index)
        if wait_for <= 0:
            break
        log.info("%s rate limit reached; waiting %.1f seconds for job #%s", bot["name"], wait_for, job_id)
        await asyncio.sleep(min(wait_for + 0.25, 60))

    forwarded_id = job.get("forwarded_message_id")
    if not forwarded_id:
        source_message = await get_source_message(job)
        if source_message is None:
            update_job(job_id, status="failed", last_error="Source message not found")
            log.error("Source message not found; job #%s marked failed", job_id)
            return

        forwarded = await client.forward_messages(LEECH_GROUP_ID, source_message)
        if isinstance(forwarded, list):
            forwarded = forwarded[0] if forwarded else None
        if forwarded is None:
            raise RuntimeError("Telegram did not return a forwarded message")
        forwarded_id = forwarded.id
        update_job(job_id, status="forwarded", forwarded_message_id=forwarded_id, last_error=None)
        log.info("Forwarded source message %s as group message %s", job["source_message_id"], forwarded_id)

    # Send the command from the logged-in Telegram USER SESSION (not from a bot token),
    # as a reply to the forwarded file so the leech bot can process that exact message.
    log.info("Sending %s from user session as reply to group message %s", bot["command"], forwarded_id)
    await client.send_message(entity=LEECH_GROUP_ID, message=bot["command"], reply_to=forwarded_id)
    record_bot_usage(bot_index)
    update_job(job_id, status="done", last_error=None)
    log.info("Job #%s completed using %s (%s)", job_id, bot["name"], bot["command"])


async def queue_worker():
    log.info("Queue worker started; limit=%s file(s) per bot per %s seconds", LIMIT_PER_BOT, WINDOW_SECONDS)
    while True:
        job = get_next_job()
        if not job:
            await asyncio.sleep(2)
            continue

        try:
            await process_job(job)
        except FloodWaitError as exc:
            delay = max(int(exc.seconds), 1)
            update_job(job["id"], last_error=f"FloodWait: {delay}s")
            log.warning("Telegram FloodWait: sleeping %s seconds", delay)
            await asyncio.sleep(delay)
        except Exception as exc:
            # Leave the job retryable. If the file was already forwarded, the saved
            # forwarded_message_id prevents forwarding it again on the next attempt.
            current = get_next_job()
            update_job(job["id"], last_error=f"{type(exc).__name__}: {exc}")
            log.exception("Job #%s failed; will retry in 10 seconds", job["id"])
            await asyncio.sleep(10)


@client.on(events.NewMessage)
async def source_listener(event):
    message = event.message
    # Temporary diagnostic logging: confirms which chat IDs this session can see.
    log.info(
        "Incoming Telegram message: chat_id=%s message_id=%s photo=%s document=%s",
        event.chat_id,
        message.id,
        bool(message.photo),
        bool(message.document),
    )

    if event.chat_id != SOURCE_CHANNEL_ID:
        return

    # Telegram files/videos/audio/documents are normally represented as document;
    # photos are handled separately. Ignore text-only posts.
    if not (message.photo or message.document):
        log.info("Source message %s ignored: no photo/document media", message.id)
        return

    inserted = enqueue_message(event.chat_id, message.id)
    if inserted:
        log.info("Queued source message %s", message.id)
    else:
        log.info("Duplicate source message %s ignored", message.id)


async def main():
    global worker_task
    init_db()
    threading.Thread(target=start_health_server, daemon=True).start()
    await client.start()
    log.info("Telegram user session connected")
    try:
        await client.send_message("me", "I'm now healthy")
        log.info("Startup health message sent to Saved Messages")
    except Exception:
        log.exception("Could not send startup health message to Saved Messages")
    log.info("Source=%s | Leech group=%s", SOURCE_CHANNEL_ID, LEECH_GROUP_ID)

    # Validate that the logged-in account can resolve both configured chats.
    try:
        source_entity = await client.get_entity(SOURCE_CHANNEL_ID)
        log.info("Source resolved: title=%s id=%s", getattr(source_entity, "title", None), getattr(source_entity, "id", None))
    except Exception:
        log.exception("SOURCE CHANNEL CHECK FAILED: account cannot resolve SOURCE_CHANNEL_ID")

    try:
        group_entity = await client.get_entity(LEECH_GROUP_ID)
        log.info("Leech group resolved: title=%s id=%s", getattr(group_entity, "title", None), getattr(group_entity, "id", None))
    except Exception:
        log.exception("LEECH GROUP CHECK FAILED: account cannot resolve LEECH_GROUP_ID")

    # Diagnostic: check that the source has recent messages visible to this session.
    try:
        recent = await client.get_messages(SOURCE_CHANNEL_ID, limit=3)
        log.info("Source history check: fetched %s message(s)", len(recent))
        for item in recent:
            log.info("Source history item: id=%s photo=%s document=%s", item.id, bool(item.photo), bool(item.document))
    except Exception:
        log.exception("SOURCE HISTORY CHECK FAILED")

    log.info("Routing: 3 files each -> Bot 1 /1, Bot 2 /l2, Bot 3 /l3; repeat after rate-limit slots expire")
    worker_task = asyncio.create_task(queue_worker())
    await client.run_until_disconnected()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Stopped")

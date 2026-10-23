# Atu-leech — Telegram User Session Auto-Leech

Automatically watches a source channel, forwards new media into your leech group, and replies to the forwarded message with the configured command.

## Routing and limits

- Files 1–3: Bot 1, command `/1`
- Files 4–6: Bot 2, command `/l2`
- Files 7–9: Bot 3, command `/l3`
- Then the same 3/3/3 routing pattern repeats.
- Each bot is limited to 3 commands in a rolling 10-minute window. If the next assigned bot is at its limit, the queue waits for that bot's oldest slot to expire; it does not skip or reorder the file.
- Duplicate source messages are ignored using a SQLite unique key.
- The command is sent as a reply to the forwarded file.
- Only source posts containing a Telegram photo or document/media are queued.

## Render setup

This repository includes `render.yaml`. Create a Render Web Service from this repository and set the secret environment variables:

- `API_ID`: Telegram API ID from my.telegram.org
- `API_HASH`: Telegram API hash from my.telegram.org
- `SESSION_STRING`: generate locally with `generate_session.py`; never paste it into chats or commit it to GitHub.

The source and group IDs and three bot commands are prefilled in `render.yaml`. You can override them in Render environment variables.

Build command: `pip install -r requirements.txt`  
Start command: `python main.py`  
Health endpoint: `/health`

## Important hosting note

Render Free web services can spin down after inactivity, and their local filesystem is not durable across all restarts/redeploys. The SQLite queue and rate-limit timestamps therefore may be lost when the instance is replaced. For dependable continuous queue processing and durable state, use persistent storage or a persistent database and a host that stays running. A health endpoint alone does not guarantee that a free instance will remain awake.

## Session security

Do not commit `SESSION_STRING`, API credentials, or Telegram OTP/passwords to this repository. Keep them only in private environment variables.

"""Run locally to create a Telethon StringSession. Never share the output publicly."""
import asyncio
import os
from telethon import TelegramClient
from telethon.sessions import StringSession

async def main():
    api_id = int(os.environ["API_ID"])
    api_hash = os.environ["API_HASH"]
    async with TelegramClient(StringSession(), api_id, api_hash) as client:
        print("\nSESSION_STRING (copy only into your hosting environment variable):\n")
        print(client.session.save())

if __name__ == "__main__":
    asyncio.run(main())

"""Explicit non-destructive repair on the same .env/engine as the running application.
Run from voice-bot with the app stopped: python repair_sheet_schema.py
No external API writes. No new environment variables.
"""
import asyncio
from pathlib import Path
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent / '.env')

async def main():
    from leads.db import ensure_call_sheet_schema, get_engine
    await ensure_call_sheet_schema()
    print('Verified call_sheet_exports exists on the active Leads engine.')
    await get_engine().dispose()

if __name__ == '__main__':
    asyncio.run(main())

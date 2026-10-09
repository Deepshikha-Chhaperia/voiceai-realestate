"""Read-only account-specific Groq model inventory. Never prints credentials."""
import asyncio
import os
from pathlib import Path
import httpx
from dotenv import load_dotenv

async def main():
    load_dotenv(Path(__file__).parent / '.env')
    key = os.getenv('GROQ_API_KEY', '').strip()
    if not key:
        raise SystemExit('GROQ_API_KEY is missing from existing app environment')
    async with httpx.AsyncClient(timeout=15) as client:
        response = await client.get('https://api.groq.com/openai/v1/models', headers={'Authorization': 'Bearer ' + key})
        response.raise_for_status()
        models = response.json().get('data', [])
    active = sorted(str(m['id']) for m in models if m.get('id') and m.get('active', True))
    if not active:
        raise SystemExit('No active models returned. Leave backup disabled.')
    print('Current account model IDs (inventory is not proof of tool capability):')
    for model in active:
        print(model)
    print('No completion requests, config writes, sends or purchases performed.')

if __name__ == '__main__':
    asyncio.run(main())

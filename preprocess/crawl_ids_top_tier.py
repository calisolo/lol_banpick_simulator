import requests
import json
import os
from typing import Dict
from dotenv import load_dotenv

# .env 파일 로드
dotenv_path = ".env"
load_dotenv(dotenv_path)
API_KEY = os.getenv("RIOT_KEY")

if not API_KEY:
    raise ValueError("❌ RIOT_KEY not found in .env file!")

# 설정
BASE_URL = "https://kr.api.riotgames.com/lol/league/v4"
QUEUE = "RANKED_SOLO_5x5"

# 저장 경로: rank_data/KR/
SAVE_DIR = os.path.join("rank_data", "KR")
os.makedirs(SAVE_DIR, exist_ok=True)

def save_top_tier_data(tier: str, data: Dict):
    filepath = os.path.join(SAVE_DIR, f"{tier}.json")
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"✅ Saved {len(data.get('entries', []))} entries to {filepath}")

def fetch_top_tier(tier: str) -> Dict:
    # 엔드포인트 매핑
    endpoint_map = {
        "CHALLENGER": f"{BASE_URL}/challengerleagues/by-queue/{QUEUE}",
        "GRANDMASTER": f"{BASE_URL}/grandmasterleagues/by-queue/{QUEUE}",
        "MASTER": f"{BASE_URL}/masterleagues/by-queue/{QUEUE}",
    }
    
    if tier not in endpoint_map:
        print(f"⚠️ Invalid tier: {tier}")
        return {}

    url = endpoint_map[tier]
    params = {"api_key": API_KEY}

    try:
        print(f"🔍 Fetching {tier} data...")
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        return data
    except requests.exceptions.RequestException as e:
        print(f"❌ Request failed for {tier}: {e}")
        return {}
    except json.JSONDecodeError:
        print(f"❌ JSON decode failed for {tier}")
        return {}

def main():
    print("🚀 Top Tier (Challenger / Grandmaster / Master) Crawler")
    print("📁 Saving to: rank_data/KR/")
    print("⚠️  Each tier is fetched in a single request.")
    print("=" * 60)

    tiers = ["CHALLENGER", "GRANDMASTER", "MASTER"]

    for tier in tiers:
        data = fetch_top_tier(tier)
        if data and "entries" in data:
            save_top_tier_data(tier, data)
        else:
            print(f"⚠️ No valid data for {tier}")

    print("\n🎉 All done!")

if __name__ == "__main__":
    main()
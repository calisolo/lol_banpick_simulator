import requests
import time
import json
import os
from typing import List, Dict
from dotenv import load_dotenv
# import pathlib
# dotenv_path = pathlib.Path.home() / "code/.env"

dotenv_path = ".env"
load_dotenv(dotenv_path)
api_key = os.getenv("RIOT_KEY")

API_KEY = api_key  # 🔑 여기에 실제 API 키 입력
BASE_URL = "https://kr.api.riotgames.com/lol/league/v4/entries/RANKED_SOLO_5x5"

TIERS = ["PLATINUM"]#["DIAMOND", "EMERALD", "PLATINUM", "GOLD", "SILVER", "BRONZE", "IRON"]
DIVISIONS = ["III", "IV"]#["I", "II", "III", "IV"]

# 저장 디렉토리 생성 (선택)
os.makedirs("rank_data", exist_ok=True)

def save_entries(tier: str, division: str, entries: List[Dict]):
    filepath = os.path.join("rank_data", f"{tier}-{division}.json")
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(entries, f, ensure_ascii=False, indent=2)
    print(f"✅ Saved {len(entries)} entries to {filepath}")

def fetch_page(tier: str, division: str, page: int) -> List[Dict]:
    url = f"{BASE_URL}/{tier}/{division}"
    params = {
        "page": page,
        "api_key": API_KEY
    }
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, list):
            print(f"⚠️ Unexpected response (not list) at {tier}/{division}/page={page}: {data}")
            return []
        return data
    except requests.exceptions.RequestException as e:
        print(f"❌ Request failed at {tier}/{division}/page={page}: {e}")
        return []
    except json.JSONDecodeError:
        print(f"❌ JSON decode failed at {tier}/{division}/page={page}")
        return []

def crawl_tier_division(tier: str, division: str):
    print(f"\n🔍 Starting {tier} {division}...")
    all_entries = []
    page = 1

    while True:
        print(f"  → Fetching page {page}...", end=" ")
        entries = fetch_page(tier, division, page)

        if not entries:  # 빈 배열이면 종료
            print("[] → End of pages.")
            break

        print(f"{len(entries)} entries")
        all_entries.extend(entries)

        # 요청 제한 준수: 1초 대기 (20/sec, 100/2min 초과 방지)
        time.sleep(1.0)

        # 다음 페이지
        page += 1

    # 저장
    if all_entries:
        save_entries(tier, division, all_entries)
    else:
        print(f"⚠️ No data for {tier} {division}")

def main():
    print("🚀 pid Crawler")
    print("⚠️  Rate limit: 20/1s, 100/2min → Using 1 request/second for safety.")
    print("=" * 60)
    
    for tier in TIERS:
        for division in DIVISIONS:
            crawl_tier_division(tier, division)

    print("\n🎉 All done!")

if __name__ == "__main__":
    main()
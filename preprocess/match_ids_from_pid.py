# python preprocess/match_ids_from_pid.py --ranks CHALLENGER GRANDMASTER MASTER DIAMOND --start-date 2025-11-07 --days 1 --max-players 5
import requests
import json
import os
import time
import shutil
from datetime import datetime, timedelta
from typing import List, Optional, Dict
from dotenv import load_dotenv
import argparse
from datasets import Dataset
from huggingface_hub import HfApi, create_repo
import tempfile
import os
from typing import List, Dict, Any
import time
import threading
from collections import deque

# ====== 전역: 2분당 100회 요청 관리 ======
REQUEST_HISTORY = deque()  # (timestamp,)
REQUEST_LOCK = threading.Lock()
MAX_REQUESTS_PER_120_SEC = 100

def upload_date_to_hf(date_str: str, hf_repo_id: str, hf_token: str):
    """
    하이브리드 업로드:
      - raw/{date_str}/*.json       : 원본 JSON (인간 친화적)
      - data/{date_str}--{name}     : Dataset (머신/분석 친화적)
    """
    date_dir = os.path.join(BASE_SAVE_DIR, "KR", date_str)
    if not os.path.exists(date_dir):
        print(f"⚠️ No data for {date_str}, skipping upload")
        return

    # 레포지토리 생성 (이미 있으면 무시)
    create_repo(repo_id=hf_repo_id, token=hf_token, repo_type="dataset", exist_ok=True)
    api = HfApi(token=hf_token)

    uploaded_any = False

    # === 1. RAW JSON 업로드 (raw/ 폴더) ===
    for fname in os.listdir(date_dir):
        if not fname.endswith(".json"):
            continue
        local_path = os.path.join(date_dir, fname)
        hf_raw_path = f"raw/{date_str}/{fname}"
        api.upload_file(
            path_or_fileobj=local_path,
            path_in_repo=hf_raw_path,
            repo_id=hf_repo_id,
            repo_type="dataset",
            commit_message=f"Add raw {fname} for {date_str}"
        )
        print(f"📄 Uploaded raw: {hf_raw_path}")
        uploaded_any = True

    # === 2. DATASET (Parquet/Arrow) 업로드 (data/ 폴더) ===
    with tempfile.TemporaryDirectory() as tmp_data_dir:
        for fname in os.listdir(date_dir):
            if not fname.endswith(".json"):
                continue

            is_unique = fname.endswith("_unique.json")
            if is_unique:
                rank_key = fname.replace("_unique.json", "")
                subfolder_name = f"{date_str}--{rank_key}_unique"
            else:
                rank_key = fname.replace(".json", "")
                subfolder_name = f"{date_str}--{rank_key}"

            # JSON 로드
            with open(os.path.join(date_dir, fname), "r", encoding="utf-8") as f:
                data = json.load(f)

            # Dataset용 행 생성
            if is_unique:
                match_ids = [mid for mid in data if mid]
                if not match_ids:
                    continue
                rows: List[Dict[str, Any]] = [{"match_id": mid} for mid in match_ids]
            else:
                rows = [
                    {"puuid": puuid, "match_ids": match_ids}
                    for puuid, match_ids in data.items()
                    if match_ids
                ]
                if not rows:
                    continue

            # Dataset 생성 및 저장
            dataset = Dataset.from_list(rows)
            dataset_path = os.path.join(tmp_data_dir, subfolder_name)
            dataset.save_to_disk(dataset_path)

        # data/ 폴더 전체 업로드
        if os.listdir(tmp_data_dir):  # 비어있지 않으면
            api.upload_folder(
                folder_path=tmp_data_dir,
                path_in_repo="data",
                repo_id=hf_repo_id,
                repo_type="dataset",
                commit_message=f"Add dataset files for {date_str}",
                ignore_patterns=[".*"]
            )
            print(f"📊 Uploaded datasets to data/ for {date_str}")
        else:
            print(f"⚠️ No valid datasets to upload for {date_str}")

    if uploaded_any:
        print(f"✅ Hybrid upload completed for {date_str}")
        # 🔥 업로드 성공 시 로컬 폴더 삭제
        try:
            shutil.rmtree(date_dir)
            print(f"🧹 Deleted local data for {date_str}")
        except Exception as e:
            print(f"⚠️ Failed to delete {date_dir}: {e}")
    else:
        print(f"⚠️ Nothing uploaded for {date_str}")
# .env 로드
load_dotenv(".env")
RIOT_API_KEY = os.getenv("RIOT_KEY")
HF_TOKEN = os.getenv("HF_TOKEN")
if not HF_TOKEN:
    raise ValueError("❌ HF_TOKEN not found in .env")
HF_REPO_ID = os.getenv("HF_REPO_ID", "your-username/lol-match-ids")
if not RIOT_API_KEY:
    raise ValueError("❌ RIOT_KEY not found in .env")

MATCH_URL_BASE = "https://asia.api.riotgames.com/lol/match/v5/matches/by-puuid"
BASE_SAVE_DIR = "match_ids"
TMP_DIR_BASE = "tmp_match_fetch"  # 임시 디렉토리

TOP_TIERS = {"CHALLENGER", "GRANDMASTER", "MASTER"}
DIVISIONS = ["I", "II", "III", "IV"]




def enforce_rate_limit():
    now = time.time()
    with REQUEST_LOCK:
        # 120초(2분) 이내 요청만 남기기
        while REQUEST_HISTORY and now - REQUEST_HISTORY[0] > 120:
            REQUEST_HISTORY.popleft()
        # 요청 제한 확인
        if len(REQUEST_HISTORY) >= MAX_REQUESTS_PER_120_SEC:
            sleep_time = 120 - (now - REQUEST_HISTORY[0]) + 1
            if sleep_time > 0:
                print(f"⏳ Rate limit reached. Sleeping {sleep_time:.1f}s...")
                time.sleep(sleep_time)
                # 재검사 (재귀적일 필요 없음, 다시 진입 시 자동 처리)
                enforce_rate_limit()
        else:
            REQUEST_HISTORY.append(now)

def fetch_match_ids_by_puuid(puuid: str, start_time: int, end_time: int, max_retries: int = 3) -> List[str]:
    url = f"{MATCH_URL_BASE}/{puuid}/ids"
    params = {
        "startTime": start_time,
        "endTime": end_time,
        "start": 0,
        "count": 100,
        "api_key": RIOT_API_KEY
    }

    for attempt in range(1, max_retries + 1):
        enforce_rate_limit()  # 2분당 300회 제한 적용
        try:
            response = requests.get(url, params=params, timeout=10)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 10))
                print(f"  ⚠️ 429 Rate limited. Waiting {retry_after}s (attempt {attempt}/{max_retries})")
                time.sleep(retry_after)
            elif response.status_code == 404:
                return []
            elif response.status_code == 500:
                print(f"  ⚠️ Server error (500). Retrying... (attempt {attempt}/{max_retries})")
                time.sleep(2 ** attempt)  # 지수 백오프
            else:
                response.raise_for_status()
        except requests.exceptions.Timeout:
            print(f"  ⚠️ Timeout (attempt {attempt}/{max_retries})")
            time.sleep(2 ** attempt)
        except requests.exceptions.RequestException as e:
            print(f"  ❌ Request error (attempt {attempt}/{max_retries}): {e}")
            time.sleep(2 ** attempt)

    print(f"  ❌ Failed to fetch matches for PUUID {puuid} after {max_retries} retries.")
    return []

def save_puuid_matches_to_tmp(rank: str, date_str: str, puuid: str, match_ids: List[str], tmp_base: str):
    tmp_dir = os.path.join(tmp_base, "KR", date_str, rank)
    os.makedirs(tmp_dir, exist_ok=True)
    tmp_path = os.path.join(tmp_dir, f"{puuid}.json")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(match_ids, f, ensure_ascii=False)
    # 디버그 출력 생략 가능


def finalize_rank_files(rank: str, date_str: str, tmp_base: str):
    tmp_dir = os.path.join(tmp_base, "KR", date_str, rank)
    if not os.path.exists(tmp_dir):
        print(f"    ⚠️ No temp files for {rank}, skipping finalization")
        return

    match_dict: Dict[str, List[str]] = {}
    all_match_ids = set()

    # 임시 파일 읽기
    for fname in os.listdir(tmp_dir):
        if not fname.endswith(".json"):
            continue
        puuid = fname[:-5]  # .json 제거
        path = os.path.join(tmp_dir, fname)
        with open(path, "r", encoding="utf-8") as f:
            matches = json.load(f)
        match_dict[puuid] = matches
        all_match_ids.update(matches)

    # 최종 저장 경로
    final_dir = os.path.join(BASE_SAVE_DIR, "KR", date_str)
    os.makedirs(final_dir, exist_ok=True)

    # {rank}.json 저장
    rank_file = os.path.join(final_dir, f"{rank}.json")
    with open(rank_file, "w", encoding="utf-8") as f:
        json.dump(match_dict, f, ensure_ascii=False, indent=2)
    print(f"    ✅ Saved {len(match_dict)} PUUIDs to {rank_file}")

    # {rank}_unique.json 저장
    unique_list = sorted(all_match_ids)
    unique_file = os.path.join(final_dir, f"{rank}_unique.json")
    with open(unique_file, "w", encoding="utf-8") as f:
        json.dump(unique_list, f, ensure_ascii=False, indent=2)
    print(f"    ✅ Saved {len(unique_list)} unique match IDs to {unique_file}")

    # 임시 폴더 정리
    shutil.rmtree(tmp_dir, ignore_errors=True)


def datetime_to_epoch(dt: datetime) -> int:
    return int(dt.timestamp())


def get_all_files_for_rank(rank: str) -> List[str]:
    rank_upper = rank.upper()
    if rank_upper in TOP_TIERS:
        return [os.path.join("user_ids", "KR", f"{rank_upper}.json")]
    else:
        files = []
        for div in DIVISIONS:
            filepath = os.path.join("user_ids", "KR", f"{rank_upper}-{div}.json")
            if os.path.exists(filepath):
                files.append(filepath)
            else:
                print(f"⚠️ Warning: {filepath} not found (skipping)")
        return files


def load_entries_from_file(filepath: str, is_top_tier: bool) -> List[Dict]:
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)
    if is_top_tier:
        if isinstance(data, dict) and "entries" in data:
            return data["entries"]
        else:
            raise ValueError(f"Expected {{'entries': [...]}} in {filepath}")
    else:
        if isinstance(data, list):
            return data
        else:
            raise ValueError(f"Expected list in {filepath}")


def get_puuid_from_entry(entry: Dict, is_top_tier: bool) -> Optional[str]:
    return entry.get("puuid")


def process_rank_on_date(rank: str, date_str: str, start_ts: int, end_ts: int, max_players: int = 10):
    rank_upper = rank.upper()
    is_top_tier = rank_upper in TOP_TIERS

    filepaths = get_all_files_for_rank(rank)
    if not filepaths:
        print(f"⚠️ No files found for rank {rank}. Skipping.")
        return

    all_entries = []
    for fp in filepaths:
        try:
            entries = load_entries_from_file(fp, is_top_tier)
            all_entries.extend(entries)
        except Exception as e:
            print(f"❌ Error loading {fp}: {e}")

    if not all_entries:
        print(f"⚠️ No valid entries found for {rank}. Skipping.")
        return

    entries_to_process = all_entries[:max_players]
    print(f"\n  🏆 Processing {rank} ({len(all_entries)} total players, using first {len(entries_to_process)})")

    tmp_base = TMP_DIR_BASE  # 전역 임시 루트

    for idx, entry in enumerate(entries_to_process, 1):
        puuid = get_puuid_from_entry(entry, is_top_tier)
        if not puuid:
            print(f"    ⚠️ Missing puuid in entry: {entry}")
            continue

        print(f"    [{idx}/{len(entries_to_process)}] PUUID: {puuid[:8]}...")

        match_ids = fetch_match_ids_by_puuid(puuid, start_ts, end_ts)
        save_puuid_matches_to_tmp(rank_upper, date_str, puuid, match_ids, tmp_base)

    # 모든 PUUID 처리 후 최종 병합
    finalize_rank_files(rank_upper, date_str, tmp_base)


# main(), argparse 등은 이전과 동일 → 생략 가능 (아래에 포함)

def main():
    parser = argparse.ArgumentParser(
        description="Crawl match IDs with low memory usage (tmp-file based)."
    )
    parser.add_argument("--ranks", nargs="+", required=True)
    parser.add_argument("--start-date", default="2025-11-07")
    parser.add_argument("--days", type=int, default=1)
    parser.add_argument("--max-players", type=int, default=10)
    args = parser.parse_args()

    ranks = [r.upper() for r in args.ranks]
    start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
    days_back = args.days
    max_players = args.max_players

    print("🚀 Starting low-memory match ID crawler")
    print(f"📌 Base ranks: {ranks}")
    print(f"📅 From {start_date.strftime('%Y-%m-%d')} backwards for {days_back} day(s)")
    print(f"👥 Max players per base rank: {max_players}")
    print("=" * 70)

    for day_offset in range(days_back):
        target_date = start_date - timedelta(days=day_offset)
        date_str = target_date.strftime("%Y-%m-%d")
        start_ts = datetime_to_epoch(target_date)
        end_ts = datetime_to_epoch(target_date + timedelta(days=1) - timedelta(seconds=1))

        print(f"\n📆 === Processing date: {date_str} ===")

        for rank in ranks:
            process_rank_on_date(rank, date_str, start_ts, end_ts, max_players)

        # ✅ 이 날짜의 모든 랭크 처리 완료 → Hugging Face 업로드
        upload_date_to_hf(date_str, HF_REPO_ID, HF_TOKEN)

    # ❌ 최종 tmp 삭제는 유지 (선택 사항)
    if os.path.exists(TMP_DIR_BASE):
        shutil.rmtree(TMP_DIR_BASE, ignore_errors=True)

    print("\n🎉 All done!")


if __name__ == "__main__":
    main()
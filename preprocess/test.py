# test_puuid.py
import requests, os
from dotenv import load_dotenv
load_dotenv()
key = os.getenv("RIOT_KEY")

# CHALLENGER.json에서 하나의 summonerId 복사 (예: "abc123...")
summoner_id = "zkADbzMvEUa1F8YjNScfCkZOMPmZerOkVnjPZLlpv-8mZRvGKNhOAHElYhq_C9EeLevNIcIYFEtG1w"
url = f"https://kr.api.riotgames.com/lol/summoner/v4/summoners/{summoner_id}"
res = requests.get(url, params={"api_key": key})
print(res.status_code)
print(res.json())
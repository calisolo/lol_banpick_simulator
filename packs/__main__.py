import pandas as pd
import time
import requests
from datetime import datetime, timedelta
from util_functions import upload_file, download_text, download_csv
from env import RIOT_TOKEN
import pandas as pd
import json
import sys

access_token = RIOT_TOKEN
api_key = "API Key"
header = {"X-Riot-Token" : access_token }


    


def league_v4_tier(tier):
    # 솔랭 기준
    queue = 'RANKED_SOLO_5x5'

    if tier == 'challenger' :
        url = f"https://kr.api.riotgames.com/lol/league/v4/challengerleagues/by-queue/{queue}"
    elif tier == 'grandmaster':
        url = f"https://kr.api.riotgames.com/lol/league/v4/grandmasterleagues/by-queue/{queue}"
    else:
        url = f"https://kr.api.riotgames.com/lol/league/v4/masterleagues/by-queue/{queue}"
        
    return requests.get(url, headers=header)

 
def summoners(encryptedSummonerId):
    url = f"https://kr.api.riotgames.com/lol/summoner/v4/summoners/{encryptedSummonerId}"
        
    return requests.get(url, headers=header)

def match_list(puuid, start, count, startTime:list, endTime:list):
    start_dt = datetime(startTime[0], startTime[1], startTime[2], 0, 0, 0)
    end_dt = datetime(endTime[0], endTime[1], endTime[2], 0, 0, 0)
    start_timestamp = int(start_dt.timestamp())
    end_timestamp = int(end_dt.timestamp())
    url = f"https://asia.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids?startTime={start_timestamp}&endTime={end_timestamp}&type=ranked&start={start}&count={count}"
    
    return requests.get(url, headers=header)

def match(matchId):
    url = f"https://asia.api.riotgames.com/lol/match/v5/matches/{matchId}"
    
    return requests.get(url, headers=header)

def extract_data(data):
    extracted_infos = []
    extracted_infos.append(data['metadata']['matchId']) #matchId
    extracted_infos.append(data['info']['gameVersion']) #game Verison
    extracted_infos.append(data['info']['gameEndTimestamp']) #gameEnd
    extracted_infos.append(data['info']['gameDuration']) #gameDuration.. filter minor games
    extracted_infos.append(data['info']['teams'][0]['win'])# blue win True or False

    blueban,redban = [],[]
    for ban in data['info']['teams'][0]['bans']:
        blueban.append(ban['championId'])
    for ban in data['info']['teams'][1]['bans']:
        redban.append(ban['championId'])
    extracted_infos.append(blueban) #blueban
    extracted_infos.append(redban) #redban

    championIds, championNames, lanes, puuids, riotIds = [],[],[],[],[]

    for player in data['info']['participants']:

        championIds.append(player['championId'])
        championNames.append(player['championName'])
        lanes.append(player[ 'individualPosition'])
        puuids.append(player['puuid'])
        riotIds.append([player['riotIdGameName'],player['riotIdTagline']])

    extracted_infos.append(championIds[:5]) #blue picks champ id
    extracted_infos.append(championIds[5:]) # red picks champ id
    extracted_infos.append(championNames[:5]) # blue champ names
    extracted_infos.append(championNames[5:]) # red champ names
    extracted_infos.append(lanes[:5]) #blue lanes (parity)
    extracted_infos.append(lanes[5:]) # red lanes (parity)
    extracted_infos.append(puuids) #player puuids in game
    extracted_infos.append(riotIds) # player riot ids in game
    gameDuration = data['info']['gameDuration'] #다시하기판별
    extracted_infos.append(gameDuration) #게임시간
    if gameDuration < 500:
        trashgameflag = 1
    elif gameDuration < 1700:
        trashgameflag = 2 
    else:
        trashgameflag = 0
    extracted_infos.append(trashgameflag) #쓰레기겜

    return extracted_infos

def get_tier_members(tierName):
    df = ''
    req = league_v4_tier(tierName)
    if req.status_code == 200:
        df = pd.DataFrame(req.json()['entries'])
    elif req.status_code ==403:
        print('check riot api')
    return df

def get_member_ids(df):
    profileList = []
    for summoner in df['summonerId']:

        req = summoners(summoner)
        
        if req.status_code == 200:
            data = req.json()
            profileList.append(data)
        elif req.status_code == 429:
            while True:
                time.sleep(5)
                req = summoners(summoner)
                if req.status_code == 200:
                    data = req.json()
                    profileList.append(data)
                    break
        elif req.status_code == 503: # 잠시 서비스를 이용하지 못하는 에러
            print('service available error')
        else:
            print(req.status_code)
    print("member_id Done")
    additional_df = pd.DataFrame(profileList)
    merged_df = pd.merge(additional_df, df, left_on="id", right_on="summonerId", how="left")
    merged_df = merged_df.drop(columns=["summonerId"])
    merged_df = merged_df.rename(columns={"id": "summonerId"})

    return merged_df

def get_match_ids(merged_df, startTime:list, endTime:list, match_num_per_user:int ):
    matches = []
    for player in merged_df['puuid']:
        req = match_list(puuid=player, start = 0, count = match_num_per_user, startTime=startTime, endTime=endTime)
        if req.status_code == 200:
            data = req.json()
            matches += data
        elif req.status_code == 429:
            while True:
                time.sleep(5)
                req = match_list(puuid=player, start = 0, count = match_num_per_user, startTime=startTime, endTime=endTime)
                if req.status_code == 200:
                    data = req.json()
                    matches += data
                    break
        else:
            print(req.status_code)
    print("get match id Done")
    return matches

def get_detail_matches(matches):
    samples = []
    for game in matches:
        req = match(game)
        if req.status_code == 200:
            data = req.json()
            samples.append(extract_data(data))
        
        elif req.status_code == 429:
            while True:
                time.sleep(5)
                req = match(game)
                if req.status_code == 200:
                    data = req.json()
                    samples.append(extract_data(data))
                    break
        else:
            print(req.status_code)

    print('get match data done')
    columns = ['matchId', 'gameVersion', 'gameEndTimestamp', 'gameDuration', 'blueWin', 'blueBan', 'redBan', 'blueChampIds', 'redChampIds', 'blueChampNames', 'redChampNames', 'blueLanes', 'redLanes', 'puuids', 'riotIds', 'gameDuration','trashGame']

    # DataFrame 생성
    match_df = pd.DataFrame(samples, columns=columns)
    return match_df

# 챌그마 가져오기
# member_df = get_tier_members(tier)
# upload_file(file_name = f'{tier}.csv', date_today = today, csv_df = member_df)
# member_ids_df = get_member_ids(member_df)
# upload_file(file_name = f'{tier}_additional.csv',date_today = today, csv_df = member_ids_df)




def run():

    start_time = time.time()
    timeout = 230
    finish = False

    while True:
        deadAtMatchList = True

        tierName = ['challenger', 'grandmaster']
        cache = download_text("5min_cache.json", file_type = 'json')
        print('loaded cache')
        if 'dead' not in cache:
            cache['dead'] = 99

        tomorrow_str = download_text("date-counter.txt")
        tomorrow = datetime.strptime(tomorrow_str, '%Y-%m-%d')
        today = tomorrow - timedelta(days=1)
        startYear = today.year
        startMonth = today.month
        startDate = today.day
        endYear = tomorrow.year
        endMonth = tomorrow.month
        endDate = tomorrow.day
        
        
        #tiergame = get_match_ids(member_ids_df, [startYear,startMonth,startDate], [endYear,endMonth,endDate], 20)

        if 'games' in cache:
            games = cache['games']
        else:
            games = []
        if 'index' in cache:
            start_index = cache['index']
        else:
            start_index = 0
        if 'tier' in cache:
            tier_index = cache['tier']
        else:
            tier_index = 0

        if cache['dead'] != 1:

            for tier_num in range(2):
                if tier_num <tier_index:
                    continue
                tier = tierName[tier_num]
                member_ids_df = download_csv(f'{tier}_additional.csv')

                for index, player in enumerate(member_ids_df['puuid']):
                    if index < start_index:
                        continue
                    req = match_list(puuid=player, start = 0, count = 20, startTime=[startYear,startMonth,startDate], endTime=[endYear,endMonth,endDate])
                    if req.status_code == 200:
                        data = req.json()
                        games += data
                    elif req.status_code == 429:
                        while True:
                            time.sleep(5)
                            req = match_list(puuid=player, start = 0, count = 20, startTime=[startYear,startMonth,startDate], endTime=[endYear,endMonth,endDate])
                            if req.status_code == 200:
                                data = req.json()
                                games += data
                                break
                    else:
                        print(req.status_code)
                    if time.time()- start_time > timeout:
                        break
            if time.time()- start_time > timeout:
                break

        deadAtMatchList = False
        games = list(set(games))

        #match_df = get_detail_matches(games)
        if 'samples' in cache:
            samples = cache['samples']
        else:
            samples = []
        if 'gameIndex' in cache:
            start_game_index = cache['gameIndex']
        else:
            start_game_index = 0


        for gameIndex,game in enumerate(games):
            if gameIndex < start_game_index:
                continue

            req = match(game)
            if req.status_code == 200:
                data = req.json()
                samples.append(extract_data(data))
            
            elif req.status_code == 429:
                while True:
                    time.sleep(5)
                    req = match(game)
                    if req.status_code == 200:
                        data = req.json()
                        samples.append(extract_data(data))
                        break
            else:
                print(req.status_code)
            if time.time()- start_time > timeout:
                break
        if time.time()- start_time > timeout:
                break

        print('get match data done')
        columns = ['matchId', 'gameVersion', 'gameEndTimestamp', 'gameDuration', 'blueWin', 'blueBan', 'redBan', 'blueChampIds', 'redChampIds', 'blueChampNames', 'redChampNames', 'blueLanes', 'redLanes', 'puuids', 'riotIds', 'gameDuration','trashGame']
        match_df = pd.DataFrame(samples, columns=columns)
        upload_file(file_name = f'matches/{startMonth}-{startDate}matches.csv', csv_df=  match_df)
        upload_file(file_name = "date-counter.txt",input_text= today.strftime('%Y-%m-%d'), file_type = 'text')
        cache = {}
        upload_file(file_name = "5min_cache.json",input_text= cache, file_type = 'json')
        finish = True
        print('uploaded well')
        break
    
    if not finish:
        if deadAtMatchList:
                cache['tier'] = tier_num
                cache['index'] = index
                games = list(set(games))
                cache['games'] = games
                cache['dead'] = 0
        else:
            games = list(set(games))
            cache['games'] = games
            cache['samples'] = samples
            cache['gameIndex'] = gameIndex
            cache['dead'] = 1
        print('timeout!')
        upload_file(file_name = "5min_cache.json",input_text= cache, file_type = 'json')

def main(args):
    run()
    return {"maybe": 'workedwell'}

if __name__ == "__main__":
    main(sys.argv[1:])
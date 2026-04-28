import requests 
import json

import os
from dotenv import load_dotenv

from datetime import date

load_dotenv(dotenv_path = "./.env")
API_KEY = os.getenv("API_KEY")
CHANNEL_HANDLE = os.getenv("CHANNEL_HANDLE")
maxResults = 50


def get_channel_id():
    try:
        url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}"

        response = requests.get(url)

        response.raise_for_status()  # Check if the request was successful

        #print(response)

        data = response.json()

        #print(json.dumps(data, indent=4))

        channel_items= data["items"][0]

        channel_playlistId = channel_items["contentDetails"]["relatedPlaylists"]['uploads']

        print(channel_playlistId)
        return channel_playlistId
    
    except requests.exceptions.RequestException as e:
        raise e

def get_video_ids(playlistId):
    
    video_ids = []

    pagetoken = None

    base_url = f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={maxResults}&playlistId={playlistId}&key={API_KEY}"

    try: 
        while True:
            url = base_url
            if pagetoken:
                url += f"&pageToken={pagetoken}"
            response = requests.get(url)
            response.raise_for_status()  # Check if the request was successful
            data = response.json()
            for item in data.get('items', []):
                video_id =item['contentDetails']['videoId']
                video_ids.append(video_id)
            pagetoken = data.get('nextPageToken')
            if not pagetoken:
                break
        return video_ids
    except requests.exceptions.RequestException as e:
        raise e

def get_video_details(video_ids):
    extracted_video_details = []
    def batch_list(video_id_lst, batch_size):
        for i in range(0, len(video_id_lst), batch_size):
            yield video_id_lst[i:i + batch_size]
    try:
        for batch in batch_list(video_ids, maxResults):
            video_ids_str = ",".join(batch)

            url = f"https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&part=snippet&part=statistics&id={video_ids_str}&key={API_KEY}"


            response = requests.get(url)

            response.raise_for_status() 

            data = response.json()

            for item in data.get('items', []):
                video_id = item['id']
                snippet = item['snippet']
                contentDetails = item['contentDetails']
                statistics = item['statistics']
            
                video_data = {
                    "video_id": video_id,
                    "title": snippet["title"],
                    "description": snippet["description"],
                    "publishedAt": snippet["publishedAt"],
                    "duration": contentDetails["duration"],
                    "viewCount": statistics.get("viewCount", None),
                    "likeCount": statistics.get("likeCount", None),
                    "commentCount": statistics.get("commentCount", None)
                }
                extracted_video_details.append(video_data)
        return extracted_video_details
    
    except requests.exceptions.RequestException as e:
        raise e

def save_to_json(extracted_data):
    os.makedirs("./data", exist_ok=True)
    file_path = f"./data/Youtube_data_{date.today()}.json"

    with open(file_path, "w", encoding="utf-8") as json_file:
        json.dump(extracted_data, json_file, indent=4, ensure_ascii=False)

if __name__ == "__main__":
    playlistId = get_channel_id()
    video_ids = get_video_ids(playlistId)
    video_data = get_video_details(video_ids)
    save_to_json(video_data)
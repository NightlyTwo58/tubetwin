import json
import csv
import time
from googleapiclient.discovery import build
from itertools import islice

API_KEY = "AIzaSyDl1kCTrO4heRfOZDI5UQ6qNm-oFRPWIto"
CHANNEL_JSON = "channels.json"
CSV_OUTPUT = "youtube_data.csv"
MAX_VIDEOS = 5
MAX_COMMENTS = 10
BATCH_SIZE = 50

with open(CHANNEL_JSON, "r", encoding="utf-8") as f:
    channel_data = json.load(f)

youtube = build("youtube", "v3", developerKey=API_KEY)

def chunked(iterable, size):
    it = iter(iterable)
    while True:
        chunk = list(islice(it, size))
        if not chunk:
            return
        yield chunk

def get_recent_videos(channel_id, max_results=MAX_VIDEOS):
    try:
        request = youtube.search().list(
            part="id",
            channelId=channel_id,
            order="date",
            type="video",
            maxResults=max_results
        )
        response = request.execute()
        return [item["id"]["videoId"] for item in response.get("items", [])]
    except Exception as e:
        print(f"Error fetching videos for {channel_id}: {e}")
        return []

def get_video_stats(video_ids):
    stats = []
    for batch in chunked(video_ids, BATCH_SIZE):
        try:
            request = youtube.videos().list(
                part="snippet,statistics",
                id=",".join(batch)
            )
            response = request.execute()
            for item in response.get("items", []):
                stats.append({
                    "video_id": item["id"],
                    "title": item["snippet"]["title"],
                    "views": int(item["statistics"].get("viewCount", 0))
                })
        except Exception as e:
            print(f"Error fetching video stats: {e}")
    return stats

def get_top_comments(video_id, max_results=MAX_COMMENTS):
    try:
        request = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            order="relevance",
            textFormat="plainText",
            maxResults=max_results
        )
        response = request.execute()
        comments = []
        for item in response.get("items", []):
            snippet = item["snippet"]["topLevelComment"]["snippet"]
            comments.append({
                "author": snippet["authorDisplayName"],
                "text": snippet["textDisplay"],
                "likes": snippet.get("likeCount", 0)
            })
        if not comments:
            comments = [{"author": "", "text": "", "likes": 0}]
        return comments
    except Exception as e:
        print(f"Error fetching comments for {video_id}: {e}")
        return [{"author": "", "text": "", "likes": 0}]

all_rows = []
for ch in channel_data:
    channel_id = ch["id"]
    subs = ch.get("subs", 0)
    total_views = ch.get("views", 0)
    video_count = ch.get("videos", 0)
    cluster = ch.get("cluster", -1)

    video_ids = get_recent_videos(channel_id)
    video_stats = get_video_stats(video_ids)

    for video in video_stats:
        comments = get_top_comments(video["video_id"])
        for comment in comments:
            all_rows.append([
                channel_id,
                subs,
                total_views,
                video_count,
                cluster,
                video["video_id"],
                video["title"],
                video["views"],
                comment["author"],
                comment["text"],
                comment["likes"]
            ])
    print(f"Completed channel {channel_id}")

header = [
    "channel_id",
    "channel_subs",
    "channel_total_views",
    "channel_video_count",
    "channel_cluster",
    "video_id",
    "video_title",
    "video_views",
    "comment_author",
    "comment_text",
    "comment_likes"
]

with open(CSV_OUTPUT, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(header)
    writer.writerows(all_rows)

print(f"Saved {len(all_rows)} rows to {CSV_OUTPUT}")

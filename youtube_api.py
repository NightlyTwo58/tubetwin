import time
from googleapiclient.discovery import build
from itertools import islice

API_KEY = "AIzaSyDl1kCTrO4heRfOZDI5UQ6qNm-oFRPWItof"
BATCH_SIZE = 50
MAX_VIDEOS = 5
MAX_COMMENTS = 10

youtube = build("youtube", "v3", developerKey=API_KEY)

def chunked(iterable, size):
    it = iter(iterable)
    while True:
        chunk = list(islice(it, size))
        if not chunk:
            return
        yield chunk

def safe_execute(request, retries=5):
    """Execute API requests with retries for quota/rate-limit handling."""
    for attempt in range(retries):
        try:
            return request.execute()
        except Exception as e:
            if "quota" in str(e).lower() or "ratelimit" in str(e).lower():
                wait = (2 ** attempt) + 0.5
                print(f"[WARN] Quota hit — sleeping {wait:.1f}s...")
                time.sleep(wait)
            else:
                print(f"[ERROR] API call failed: {e}")
                return {}
    return {}

def get_recent_videos(channel_id, last_checked=None, max_results=MAX_VIDEOS):
    params = {
        "part": "id",
        "channelId": channel_id,
        "order": "date",
        "type": "video",
        "maxResults": max_results
    }
    if last_checked:
        params["publishedAfter"] = last_checked

    response = safe_execute(youtube.search().list(**params))
    return [item["id"]["videoId"] for item in response.get("items", [])]

def get_video_stats(video_ids):
    stats = []
    for batch in chunked(video_ids, BATCH_SIZE):
        response = safe_execute(
            youtube.videos().list(
                part="snippet,statistics",
                id=",".join(batch)
            )
        )
        for item in response.get("items", []):
            s = item.get("statistics", {})
            stats.append({
                "video_id": item["id"],
                "title": item["snippet"]["title"],
                "views": int(s.get("viewCount", 0)),
                "commentCount": int(s.get("commentCount", 0))
            })
    return stats

def get_top_comments(video_id, max_results=MAX_COMMENTS):
    response = safe_execute(
        youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            order="relevance",
            textFormat="plainText",
            maxResults=max_results
        )
    )
    comments = []
    for item in response.get("items", []):
        snippet = item["snippet"]["topLevelComment"]["snippet"]
        comments.append({
            "text": snippet["textDisplay"],
            "likes": snippet.get("likeCount", 0)
        })
    return comments or [{"text": "", "likes": 0}]

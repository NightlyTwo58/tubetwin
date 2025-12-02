import time
from googleapiclient.discovery import build

API_KEY = "AIzaSyDl1kCTrO4heRfOZDI5UQ6qNm-oFRPWIto"
MAX_VIDEOS = 50
MAX_COMMENTS = 10

youtube = build("youtube", "v3", developerKey=API_KEY)

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
    """Fetch videos published after last_checked (ISO string)."""
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
    """Fetch video snippet and statistics; YouTube API allows up to 50 ids per request."""
    stats = []
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i + 50]
        response = safe_execute(
            youtube.videos().list(
                part="snippet,statistics,topicDetails",
                id=",".join(batch)
            )
        )
        for item in response.get("items", []):
            s = item.get("statistics", {})
            snippet = item.get("snippet", {})
            topics = item.get("topicDetails", {}).get("topicCategories", [])
            stats.append({
                "video_id": item["id"],
                "title": snippet.get("title", ""),
                "description": snippet.get("description", ""),
                "views": int(s.get("viewCount", 0)),
                "commentCount": int(s.get("commentCount", 0)),
                "publishedAt": snippet.get("publishedAt"),
                "topics": topics
            })
    return stats

def get_channel_details(channel_ids):
    """Fetch snippet info for channels (title, description, topicDetails)."""
    details = {}
    for i in range(0, len(channel_ids), 50):
        batch = channel_ids[i:i + 50]
        response = safe_execute(
            youtube.channels().list(
                part="snippet,topicDetails",
                id=",".join(batch)
            )
        )
        for item in response.get("items", []):
            snippet = item.get("snippet", {})
            topic = item.get("topicDetails", {}).get("topicCategories", [])
            details[item["id"]] = {
                "title": snippet.get("title", ""),
                "description": snippet.get("description", ""),
                "topics": topic
            }
    return details

def get_top_comments(video_id, max_results=MAX_COMMENTS):
    """Fetch top-level comments, ordered by relevance, plain text only."""
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

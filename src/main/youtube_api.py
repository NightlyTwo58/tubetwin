from googleapiclient.discovery import build

API_KEY = "AIzaSyBq9R-lUGNH3niJiJHRrcUihKqRP-40V7c"
MAX_VIDEOS = 50
MAX_COMMENTS = 10

youtube = build("youtube", "v3", developerKey=API_KEY)

def safe_execute(func):
    """Execute a YouTube API call safely; return None if quota/rate limit hit."""
    try:
        return func()
    except Exception as e:
        msg = str(e).lower()
        if "quota" in msg or "ratelimit" in msg:
            print("[WARN] Quota hit — returning None")
            return None
        print(f"[ERROR] API call failed: {e}")
        return None

def get_recent_videos(channel_id, published_after=None, published_before=None, max_results=MAX_VIDEOS):
    """Fetch video IDs for a channel within a date range."""
    params = {
        "part": "id",
        "channelId": channel_id,
        "order": "date",
        "type": "video",
        "maxResults": max_results
    }
    if published_after:
        params["publishedAfter"] = published_after
    if published_before:
        params["publishedBefore"] = published_before

    response = safe_execute(lambda: youtube.search().list(**params).execute())
    if not response:
        return []
    return [item["id"]["videoId"] for item in response.get("items", [])]

def get_video_stats(video_ids):
    """Fetch snippet, statistics, and topic info for up to 50 videos per request."""
    stats = []
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i+50]
        response = safe_execute(lambda: youtube.videos().list(
            part="snippet,statistics,topicDetails",
            id=",".join(batch)
        ).execute())
        if not response:
            continue

        for item in response.get("items", []):
            snippet = item.get("snippet", {})
            s = item.get("statistics", {})
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
    """Fetch title, description, and topic categories for channels."""
    details = {}
    for i in range(0, len(channel_ids), 50):
        batch = channel_ids[i:i+50]
        response = safe_execute(lambda: youtube.channels().list(
            part="snippet,topicDetails",
            id=",".join(batch)
        ).execute())
        if not response:
            continue

        for item in response.get("items", []):
            snippet = item.get("snippet", {})
            topics = item.get("topicDetails", {}).get("topicCategories", [])
            details[item["id"]] = {
                "title": snippet.get("title", ""),
                "description": snippet.get("description", ""),
                "topics": topics
            }
    return details

def get_top_comments(video_id, max_results=MAX_COMMENTS):
    """Fetch top-level comments for a video (plain text, relevance order)."""
    def request():
        return youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            order="relevance",
            textFormat="plainText",
            maxResults=max_results
        ).execute()

    try:
        response = request()
    except Exception as e:
        # Ignore if comments are disabled
        if "commentsDisabled" in str(e):
            return [{"text": "", "likes": 0}]
        # Ignore quota/rate limit errors separately
        if "quota" in str(e).lower() or "ratelimit" in str(e).lower():
            print("[WARN] Quota hit while fetching comments.")
            return None
        # Otherwise, raise unexpected errors
        print(f"[ERROR] Failed to fetch comments for {video_id}: {e}")
        return [{"text": "", "likes": 0}]

    if not response or "items" not in response:
        return [{"text": "", "likes": 0}]

    comments = []
    for item in response.get("items", []):
        snippet = item["snippet"]["topLevelComment"]["snippet"]
        comments.append({
            "text": snippet.get("textDisplay", ""),
            "likes": snippet.get("likeCount", 0)
        })
    return comments or [{"text": "", "likes": 0}]


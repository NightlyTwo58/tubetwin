import json
import pandas as pd
from tqdm import tqdm
from googleapiclient.discovery import build

def load_config(path="config.json"):
    with open(path) as f:
        return json.load(f)

def get_videos(youtube, category, region, order, max_results=30):
    """Fetch video results and return associated channel IDs."""
    request = youtube.search().list(
        part="snippet",
        type="video",
        videoCategoryId=category,
        regionCode=region,
        order=order,
        maxResults=max_results
    )
    response = request.execute()
    items = response.get("items", [])
    return [(item["snippet"]["channelId"], category, region, order) for item in items]

def get_channel_data(youtube, channel_batches):
    """Fetch channel metadata for each batch of IDs."""
    data = []
    for ids, category, region, order in tqdm(channel_batches, desc="Fetching details"):
        try:
            req = youtube.channels().list(
                part="snippet,statistics",
                id=",".join(ids)
            )
            res = req.execute()
            for item in res.get("items", []):
                snippet = item["snippet"]
                stats = item.get("statistics", {})
                data.append({
                    "channel_id": item["id"],
                    "country": snippet.get("country"),
                    "view_count": stats.get("viewCount"),
                    "subscriber_count": stats.get("subscriberCount"),
                    "video_count": stats.get("videoCount"),
                    "category": category,
                    "region": region,
                    "order": order
                })
        except Exception as e:
            print(f"Error fetching batch: {e}")
    return data

def main():
    config = load_config()
    youtube = build("youtube", "v3", developerKey=config["api_key"])

    raw_channels = []
    for region in tqdm(config["regions"], desc="Regions"):
        for category in config["categories"]:
            for order in config["orders"]:
                try:
                    results = get_videos(youtube, category, region, order)
                    raw_channels.extend(results)
                except Exception as e:
                    print(f"Error {region}-{category}-{order}: {e}")

    seen = set()
    channel_batches = []
    current_batch, meta = [], None

    for ch_id, category, region, order in raw_channels:
        if ch_id not in seen:
            seen.add(ch_id)
            current_batch.append(ch_id)
            meta = (category, region, order)
            if len(current_batch) == 50:
                channel_batches.append((current_batch, *meta))
                current_batch = []

    if current_batch:
        channel_batches.append((current_batch, *meta))

    print(f"Total unique channels: {len(seen)}")

    data = get_channel_data(youtube, channel_batches)

    df = pd.DataFrame(data)
    df.to_csv("channels.csv", index=False)
    print("Saved!")

if __name__ == "__main__":
    main()

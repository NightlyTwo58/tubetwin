import csv
import json
import os
from concurrent.futures import ThreadPoolExecutor

from youtube_api import (
    get_recent_videos,
    get_video_stats,
    get_top_comments
)
from timing_utils import (
    load_cache,
    save_cache,
    needs_update,
    update_cache_entry,
    write_channel_summary_csv
)

CHANNEL_JSON = "channels.json"
COMMENTS_CSV = "youtube_data.csv"
CHANNELS_CSV = "channels_data.csv"
MAX_WORKERS = 5

def process_channel(ch, cache):
    """Fetch recent videos and comments for a single channel."""
    channel_id = ch["id"]
    subs = ch.get("subs", 0)
    total_views = ch.get("views", 0)
    video_count = ch.get("videos", 0)
    cluster = ch.get("cluster", -1)
    last_checked = cache.get(channel_id, {}).get("last_checked")

    if not needs_update(channel_id, cache):
        print(f"Skipping {channel_id} (checked recently)")
        return []

    print(f"Updating {channel_id}...")
    video_ids = get_recent_videos(channel_id, last_checked)
    if not video_ids:
        print(f"No new videos for {channel_id}")
        update_cache_entry(cache, channel_id, [])
        return []

    video_stats = get_video_stats(video_ids)
    all_rows = []

    for video in video_stats:
        comments = get_top_comments(video["video_id"]) if video["commentCount"] > 0 else [{"text": "", "likes": 0}]
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
                comment["text"],
                comment["likes"]
            ])

    update_cache_entry(cache, channel_id, [v["video_id"] for v in video_stats])
    return all_rows

def main():
    # Load channel data
    with open(CHANNEL_JSON, "r", encoding="utf-8") as f:
        channel_data = json.load(f)

    # Load or create cache
    cache = load_cache()

    all_rows = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = executor.map(lambda c: process_channel(c, cache), channel_data)
        for res in results:
            all_rows.extend(res)

    # Write or append to comment-level CSV
    if all_rows:
        write_comments_csv(all_rows)
        print(f"Saved {len(all_rows)} comment rows.")
    else:
        print("No new comment data to save.")

    # Write channel summary + cache
    write_channel_summary_csv(cache, CHANNELS_CSV)
    save_cache(cache)
    print("Update complete.")

def write_comments_csv(all_rows):
    header = [
        "channel_id",
        "channel_subs",
        "channel_total_views",
        "channel_video_count",
        "channel_cluster",
        "video_id",
        "video_title",
        "video_views",
        "comment_text",
        "comment_likes"
    ]
    file_exists = os.path.exists(COMMENTS_CSV)
    with open(COMMENTS_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists or os.stat(COMMENTS_CSV).st_size == 0:
            writer.writerow(header)
        writer.writerows(all_rows)

if __name__ == "__main__":
    main()

import csv
import json
import os

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

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "...")
CHANNEL_JSON = os.path.join(DATA_DIR, "data/input/channels.json")
COMMENTS_CSV = os.path.join(DATA_DIR, "data/output/youtube_data.csv")
CHANNELS_CSV = os.path.join(DATA_DIR, "data/output/channels_data.csv")
CACHE_JSON =  os.path.join(DATA_DIR, "data/input/cache.json")

def process_channel(ch, cache):
    """Fetch recent videos and comments for a single channel without duplicates."""
    channel_id = ch["id"]
    subs = ch.get("subs", 0)
    total_views = ch.get("views", 0)
    video_count = ch.get("videos", 0)
    cluster = ch.get("cluster", -1)
    last_checked = cache.get(channel_id, {}).get("last_checked")
    already_seen_videos = set(cache.get(channel_id, {}).get("video_ids", []))

    if not needs_update(channel_id, cache):
        print(f"Skipping {channel_id} (checked recently)")
        return []

    print(f"Updating {channel_id}...")
    video_ids = get_recent_videos(channel_id, last_checked)
    if not video_ids:
        print(f"No new videos for {channel_id}")
        update_cache_entry(cache, channel_id, [])  # still update last_checked
        return []

    # Filter out videos we already processed
    new_video_ids = [v for v in video_ids if v not in already_seen_videos]
    if not new_video_ids:
        print(f"All videos for {channel_id} already processed.")
        update_cache_entry(cache, channel_id, list(already_seen_videos))
        return []

    video_stats = get_video_stats(new_video_ids)
    all_rows = []

    for video in video_stats:
        comments = get_top_comments(video["video_id"]) if video.get("commentCount", 0) > 0 else [{"text": "", "likes": 0}]
        for comment in comments:
            all_rows.append([
                channel_id,
                subs,
                total_views,
                video_count,
                cluster,
                video["video_id"],
                video.get("title", ""),
                video.get("views", 0),
                comment.get("text", ""),
                comment.get("likes", 0)
            ])

    update_cache_entry(cache, channel_id, list(already_seen_videos | set(new_video_ids)))
    return all_rows

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

def main():
    with open(CHANNEL_JSON, "r", encoding="utf-8") as f:
        channel_data = json.load(f)

    cache = load_cache(CACHE_JSON)

    all_rows = []
    for ch in channel_data:
        rows = process_channel(ch, cache)
        if rows:
            all_rows.extend(rows)

    if all_rows:
        write_comments_csv(all_rows)
        print(f"Saved {len(all_rows)} comment rows.")
    else:
        print("No new comment data to save.")

    write_channel_summary_csv(cache, CHANNELS_CSV)
    save_cache(cache, CACHE_JSON)
    print("Update complete.")

if __name__ == "__main__":
    main()

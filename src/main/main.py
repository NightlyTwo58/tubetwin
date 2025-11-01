import csv
import json
import os
import datetime as dt
from youtube_api import get_recent_videos, get_video_stats, get_top_comments
from timing_utils import load_cache, save_cache, update_cache_entry

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", "..", "data"))

CHANNEL_JSON = os.path.join(DATA_DIR, "input", "channels.json")
CACHE_JSON = os.path.join(DATA_DIR, "input", "cache.json")
COMMENTS_CSV = os.path.join(DATA_DIR, "output", "comments_data.csv")
CHANNELS_CSV = os.path.join(DATA_DIR, "output", "channels_data.csv")

START_DATE = "2016-01-01T00:00:00Z"

def process_channel(ch, cache):
    channel_id = ch["id"]
    subs = ch.get("subs", 0)
    total_views = ch.get("views", 0)
    video_count = ch.get("videos", 0)
    cluster = ch.get("cluster", -1)

    last_checked = cache.get(channel_id, {}).get("last_checked", START_DATE)
    already_seen_videos = set(cache.get(channel_id, {}).get("video_ids", []))

    print(f"Updating {channel_id}...")
    video_ids = get_recent_videos(channel_id, last_checked)
    if not video_ids:
        print(f"No new videos for {channel_id}")
        return [], (channel_id, subs, total_views, video_count, cluster), already_seen_videos, last_checked

    new_video_ids = [v for v in video_ids if v not in already_seen_videos]
    if not new_video_ids:
        print(f"All videos for {channel_id} already processed.")
        return [], (channel_id, subs, total_views, video_count, cluster), already_seen_videos, last_checked

    video_stats = get_video_stats(new_video_ids)
    all_rows = []

    # track the latest publishedAt across processed videos
    max_date = last_checked
    for video in video_stats:
        pub = video["publishedAt"]
        if pub > max_date:
            max_date = pub

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

    # update cache using latest actual video date
    update_cache_entry(cache, channel_id, already_seen_videos | set(new_video_ids), max_date)
    return all_rows, (channel_id, subs, total_views, video_count, cluster), already_seen_videos | set(new_video_ids), max_date

def write_comments_csv(all_rows):
    header = ["channel_id","channel_subs","channel_total_views","channel_video_count","channel_cluster",
              "video_id","video_title","video_views","comment_text","comment_likes"]
    file_exists = os.path.exists(COMMENTS_CSV)
    with open(COMMENTS_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists or os.stat(COMMENTS_CSV).st_size == 0:
            writer.writerow(header)
        writer.writerows(all_rows)

def write_channel_stats_csv(channel_summaries):
    header = ["date","channel_id","channel_subs","channel_total_views","channel_video_count","channel_cluster"]
    today = dt.datetime.now().strftime("%Y-%m-%d")
    file_exists = os.path.exists(CHANNELS_CSV)
    with open(CHANNELS_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists or os.stat(CHANNELS_CSV).st_size == 0:
            writer.writerow(header)
        for ch in channel_summaries:
            writer.writerow([today, *ch])

def main():
    with open(CHANNEL_JSON, "r", encoding="utf-8") as f:
        channel_data = json.load(f)

    cache = load_cache(CACHE_JSON)
    all_rows = []
    channel_summaries = []

    for ch in channel_data:
        rows, summary, vids, last_checked = process_channel(ch, cache)
        if rows:
            all_rows.extend(rows)
        if summary:
            channel_summaries.append(summary)

    if all_rows:
        write_comments_csv(all_rows)
        print(f"Saved {len(all_rows)} comment rows.")

    if channel_summaries:
        write_channel_stats_csv(channel_summaries)
        print(f"Saved {len(channel_summaries)} channel stats entries.")

    save_cache(cache, CACHE_JSON)
    print("Update complete.")

if __name__ == "__main__":
    main()

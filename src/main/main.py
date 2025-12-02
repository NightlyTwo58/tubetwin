import csv
import json
import os
import datetime as dt
import sys

from youtube_api import get_recent_videos, get_video_stats, get_top_comments, get_channel_details, safe_execute
from timing_utils import load_cache, save_cache, update_cache_entry, parse_timestamp

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", "..", "data"))

CHANNEL_JSON = os.path.join(DATA_DIR, "input", "channels.json")
CACHE_JSON = os.path.join(DATA_DIR, "input", "cache.json")
COMMENTS_CSV = os.path.join(DATA_DIR, "output", "comments_data.csv")
CHANNELS_CSV = os.path.join(DATA_DIR, "output", "channels_data.csv")

START_DATE = "2016-01-01T00:00:00Z"
STEP_DAYS = 1

def crawl_all_channels_layered(channel_data, cache, step_days=STEP_DAYS):
    now = dt.datetime.now(dt.timezone.utc)
    # Start from the earliest last_checked among all channels
    current_day = min(
        parse_timestamp(cache.get(ch["id"], {}).get("last_checked", START_DATE))
        for ch in channel_data
    )

    while current_day < now:
        period_end = min(current_day + dt.timedelta(days=step_days), now)
        print(f"Processing {current_day.date()} → {period_end.date()}")

        all_comment_rows = []
        all_channel_summaries = []

        for ch in channel_data:
            channel_id = ch["id"]
            last_checked = parse_timestamp(
                cache.get(channel_id, {}).get("last_checked", START_DATE)
            )
            if last_checked >= period_end:
                continue  # Already processed

            already_seen_videos = set(cache.get(channel_id, {}).get("video_ids", []))

            # Fetch new videos for this day layer
            try:
                video_ids = get_recent_videos(
                    channel_id,
                    published_after=current_day.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    published_before=period_end.strftime("%Y-%m-%dT%H:%M:%SZ")
                )
            except Exception as e:
                print(f"[WARN] Error fetching videos for {channel_id}: {e}")
                save_cache(cache, CACHE_JSON)
                return

            if not video_ids:
                update_cache_entry(cache, channel_id, already_seen_videos,
                                   period_end.strftime("%Y-%m-%dT%H:%M:%SZ"))
                continue

            new_videos = [v for v in video_ids if v not in already_seen_videos]
            if not new_videos:
                update_cache_entry(cache, channel_id, already_seen_videos,
                                   period_end.strftime("%Y-%m-%dT%H:%M:%SZ"))
                continue

            # Video stats
            video_stats = safe_execute(lambda: get_video_stats(new_videos))
            if video_stats is None:
                save_cache(cache, CACHE_JSON)
                sys.exit("[WARN] Quota hit during video stats, stopping.")

            # Channel details
            ch_info = safe_execute(lambda: get_channel_details([channel_id]))
            if ch_info is None:
                save_cache(cache, CACHE_JSON)
                sys.exit("[WARN] Quota hit during channel details, stopping.")
            ch_info = ch_info.get(channel_id, {})

            # Gather comments
            for video in video_stats:
                comments = safe_execute(lambda: get_top_comments(video["video_id"]))
                if comments is None:
                    save_cache(cache, CACHE_JSON)
                    sys.exit(f"[WARN] Quota hit during comments for video {video['video_id']}, stopping.")
                if not comments:
                    comments = [{"text": "", "likes": 0}]

                for c in comments:
                    all_comment_rows.append([
                        period_end.date().isoformat(),
                        channel_id,
                        ch_info.get("title", ""),
                        ch_info.get("description", ""),
                        ", ".join(ch_info.get("topics", [])),
                        ch.get("subs", 0),
                        ch.get("views", 0),
                        ch.get("videos", 0),
                        ch.get("cluster", -1),
                        video["video_id"],
                        video.get("title", ""),
                        video.get("description", ""),
                        ", ".join(video.get("topics", [])),
                        video.get("views", 0),
                        c.get("text", ""),
                        c.get("likes", 0)
                    ])

            already_seen_videos.update(new_videos)
            update_cache_entry(cache, channel_id, already_seen_videos,
                               period_end.strftime("%Y-%m-%dT%H:%M:%SZ"))

            all_channel_summaries.append((
                channel_id,
                ch.get("subs", 0),
                ch.get("views", 0),
                ch.get("videos", 0),
                ch.get("cluster", -1)
            ))

        # Write CSVs once per day layer
        if all_comment_rows:
            write_comments_csv(all_comment_rows)
            print(f"Saved {len(all_comment_rows)} comment rows for {current_day.date()}")
        if all_channel_summaries:
            write_channel_stats_csv(all_channel_summaries)
            print(f"Saved {len(all_channel_summaries)} channel summaries for {current_day.date()}")

        save_cache(cache, CACHE_JSON)
        current_day = period_end

def write_comments_csv(rows):
    header = ["date", "channel_id", "channel_title", "channel_description", "channel_topics",
              "channel_subs", "channel_total_views", "channel_video_count", "channel_cluster",
              "video_id", "video_title", "video_description", "video_topics",
              "video_views", "comment_text", "comment_likes"]
    file_exists = os.path.exists(COMMENTS_CSV)
    with open(COMMENTS_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists or os.stat(COMMENTS_CSV).st_size == 0:
            writer.writerow(header)
        writer.writerows(rows)

def write_channel_stats_csv(summaries):
    header = ["date", "channel_id", "channel_subs", "channel_total_views", "channel_video_count", "channel_cluster"]
    today = dt.datetime.now().strftime("%Y-%m-%d")
    file_exists = os.path.exists(CHANNELS_CSV)
    with open(CHANNELS_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists or os.stat(CHANNELS_CSV).st_size == 0:
            writer.writerow(header)
        for s in summaries:
            writer.writerow([today, *s])

def main():
    with open(CHANNEL_JSON, "r", encoding="utf-8") as f:
        channel_data = json.load(f)

    cache = load_cache(CACHE_JSON)
    crawl_all_channels_layered(channel_data, cache)
    print("Update complete.")

if __name__ == "__main__":
    main()

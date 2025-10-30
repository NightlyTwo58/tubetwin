import datetime as dt
import json
import os

UPDATE_INTERVAL_HOURS = 24
CACHE_FILE = "cache.json"

def load_cache():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_cache(cache):
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2)

def needs_update(channel_id, cache):
    """Return True if a channel should be re-fetched."""
    if channel_id not in cache:
        return True
    last_check = dt.datetime.fromisoformat(cache[channel_id]["last_checked"])
    return (dt.datetime.now() - last_check).total_seconds() > UPDATE_INTERVAL_HOURS * 3600

def update_cache_entry(cache, channel_id, video_ids):
    """Update or create cache entry with current timestamp and videos."""
    cache[channel_id] = {
        "last_checked": dt.datetime.now().isoformat(),
        "videos": video_ids
    }

def write_channel_summary_csv(cache, output_file):
    """Write summary CSV for channels and timestamps."""
    import csv
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["channel_id", "last_checked", "cached_videos"])
        for cid, entry in cache.items():
            writer.writerow([cid, entry["last_checked"], ";".join(entry.get("videos", []))])

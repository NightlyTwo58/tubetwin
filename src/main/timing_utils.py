import json
import os

def load_cache(filename):
    if os.path.exists(filename):
        with open(filename, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_cache(cache, filename):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2)

def update_cache_entry(cache, channel_id, video_ids, last_checked):
    """
    Update cache for a channel:
    - 'video_ids' is the set of all videos seen so far.
    - 'last_checked' is the publishedAt of the latest processed video.
    """
    cache[channel_id] = {
        "last_checked": last_checked,
        "video_ids": sorted(list(video_ids))
    }

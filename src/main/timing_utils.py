import json
import os
import datetime as dt

def load_cache(filename):
    """Load JSON cache from file, return empty dict if not exists."""
    if os.path.exists(filename):
        with open(filename, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_cache(cache, filename):
    """Save cache dict to JSON file."""
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2)

def update_cache_entry(cache, channel_id, video_ids, last_checked):
    """Update cache for a channel."""
    cache[channel_id] = {
        "last_checked": last_checked,
        "video_ids": sorted(list(video_ids))
    }

def parse_timestamp(ts):
    """Convert ISO8601 timestamp to datetime with timezone."""
    if not ts:
        return dt.datetime.min.replace(tzinfo=dt.timezone.utc)

    # Normalize multiple '+00:00' or trailing 'Z'
    ts = ts.replace("+00:00+00:00", "+00:00")
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"

    return dt.datetime.fromisoformat(ts)

import time, random
import pandas as pd, numpy as np
from googleapiclient.discovery import build
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

API_KEY = "AIzaSyDl1kCTrO4heRfOZDI5UQ6qNm-oFRPWIto"
youtube = build("youtube", "v3", developerKey=API_KEY)

def get_channels(q, n=50):
    r = youtube.search().list(part="snippet", type="channel", q=q, regionCode="US", maxResults=n).execute()
    return [i["snippet"]["channelId"] for i in r.get("items", [])]

def get_stats(ids):
    data = []
    for i in range(0, len(ids), 50):
        r = youtube.channels().list(part="statistics,snippet", id=",".join(ids[i:i+50])).execute()
        for c in r.get("items", []):
            s = c["statistics"]
            data.append({
                "id": c["id"],
                "subs": int(s.get("subscriberCount", 0)),
                "views": int(s.get("viewCount", 0)),
                "videos": int(s.get("videoCount", 0))
            })
    return data

qs = ["gaming","music","news","tech","education","sports","travel","comedy","beauty","finance"]
ids = set()
for q in qs:
    ids |= set(get_channels(q, 50))
    time.sleep(1)
ids = list(ids)[:500]

ch = pd.DataFrame(get_stats(ids))
X = ch[["subs","views","videos"]].replace(0,1)
X = np.log1p(X)
X = StandardScaler().fit_transform(X)
kmeans = KMeans(n_clusters=5, random_state=42).fit(X)
ch["cluster"] = kmeans.labels_

samples = ch.groupby("cluster").apply(lambda x: x.sample(min(len(x), 20), random_state=1)).reset_index(drop=True)
samples.to_csv("channels.csv", index=False)
samples.to_json("channels.json", orient="records", indent=2)
print("Saved", len(samples), "sampled channels.")

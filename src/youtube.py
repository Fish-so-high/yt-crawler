import time
import requests
from .quota import QuotaExceeded


class CommentsDisabled(Exception):
    pass


class YouTube:
    BASE = "https://www.googleapis.com/youtube/v3"

    def __init__(self, api_key, ledger):
        if not api_key:
            raise SystemExit("Thiếu YOUTUBE_API_KEY trong file .env")
        self.key, self.ledger, self.s = api_key, ledger, requests.Session()

    @staticmethod
    def _reason(r):
        try:
            return r.json()["error"]["errors"][0]["reason"]
        except Exception:
            return ""

    def _call(self, method, endpoint, **params):
        self.ledger.charge(method)
        params["key"] = self.key
        for attempt in range(4):
            try:
                r = self.s.get(f"{self.BASE}/{endpoint}", params=params, timeout=30)
            except requests.RequestException:
                time.sleep(2 ** attempt)
                continue
            if r.status_code == 200:
                time.sleep(0.05)
                return r.json()
            reason = self._reason(r)
            if r.status_code == 403 and reason in ("quotaExceeded", "dailyLimitExceeded"):
                self.ledger.mark_exhausted()
                raise QuotaExceeded(reason)
            if reason in ("commentsDisabled", "videoNotFound"):
                raise CommentsDisabled(reason)
            if r.status_code in (429, 500, 502, 503, 504) or reason == "rateLimitExceeded":
                time.sleep(2 ** attempt * 2)
                continue
            r.raise_for_status()
        raise RuntimeError(f"{endpoint}: thất bại sau 4 lần thử")

    def channel(self, handle=None, channel_id=None):
        p = {"part": "snippet,contentDetails"}
        p.update({"forHandle": handle} if handle else {"id": channel_id})
        items = self._call("channels.list", "channels", **p).get("items", [])
        return items[0] if items else None

    def uploads(self, playlist_id):
        token = None
        while True:
            d = self._call("playlistItems.list", "playlistItems", part="snippet,contentDetails",
                           playlistId=playlist_id, maxResults=50, pageToken=token)
            yield from d.get("items", [])
            token = d.get("nextPageToken")
            if not token:
                return

    def videos(self, ids):
        return self._call("videos.list", "videos", part="snippet,statistics", id=",".join(ids)).get("items", [])

    def comment_threads(self, video_id, token=None):
        # textFormat=plainText: API key không đọc được textOriginal, dùng textDisplay dạng thuần
        return self._call("commentThreads.list", "commentThreads", part="snippet,replies",
                          videoId=video_id, maxResults=100, order="time",
                          textFormat="plainText", pageToken=token)

    def all_replies(self, parent_id):
        out, token = [], None
        while True:
            d = self._call("comments.list", "comments", part="snippet", parentId=parent_id,
                           maxResults=100, textFormat="plainText", pageToken=token)
            out += d.get("items", [])
            token = d.get("nextPageToken")
            if not token:
                return out

    def search_videos(self, **params):
        return self._call("search.list", "search", part="snippet", type="video", maxResults=50, **params)

"""Sổ cái hạn mức: đếm đơn vị TRƯỚC mỗi request (request lỗi cũng tốn >= 1 đơn vị).
Hạn mức YouTube reset lúc nửa đêm giờ Thái Bình Dương (PT) nên khóa ngày theo PT."""
from datetime import datetime
from zoneinfo import ZoneInfo

PT = ZoneInfo("America/Los_Angeles")

COSTS = {
    "channels.list": 1, "playlistItems.list": 1, "videos.list": 1,
    "commentThreads.list": 1, "comments.list": 1, "search.list": 100,
}


class QuotaExceeded(Exception):
    pass


class QuotaLedger:
    def __init__(self, conn, daily_limit=10000, safety_ratio=0.85, search_cap=80):
        self.conn = conn
        self.limit = int(daily_limit * safety_ratio)
        self.search_cap = search_cap
        self.run_limit = None
        self.run_used = 0

    @staticmethod
    def day():
        return datetime.now(PT).strftime("%Y-%m-%d")

    def start_run(self, budget):
        self.run_limit, self.run_used = budget, 0

    def _row(self):
        d = self.day()
        self.conn.execute("INSERT OR IGNORE INTO quota(day) VALUES(?)", (d,))
        return self.conn.execute("SELECT * FROM quota WHERE day=?", (d,)).fetchone()

    def charge(self, method, n=1):
        cost = COSTS.get(method, 1) * n
        row = self._row()
        if row["exhausted"]:
            raise QuotaExceeded("API báo hết hạn mức hôm nay")
        if row["units"] + cost > self.limit:
            raise QuotaExceeded(f"chạm ngưỡng ngày ({row['units']}/{self.limit})")
        if method == "search.list" and row["search_calls"] >= self.search_cap:
            raise QuotaExceeded("hết số lần search.list cho phép hôm nay")
        if self.run_limit is not None and self.run_used + cost > self.run_limit:
            raise QuotaExceeded("hết ngân sách của lần chạy này")
        self.conn.execute(
            "UPDATE quota SET units=units+?, search_calls=search_calls+? WHERE day=?",
            (cost, 1 if method == "search.list" else 0, row["day"]))
        self.conn.commit()
        self.run_used += cost

    def mark_exhausted(self):
        self._row()
        self.conn.execute("UPDATE quota SET exhausted=1 WHERE day=?", (self.day(),))
        self.conn.commit()

    def status(self):
        r = self._row()
        return {"day_pt": r["day"], "used": r["units"], "limit": self.limit,
                "search_calls": r["search_calls"], "exhausted": bool(r["exhausted"])}

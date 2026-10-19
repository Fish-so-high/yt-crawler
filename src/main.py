import argparse
import logging
from datetime import datetime, timezone
from apscheduler.schedulers.blocking import BlockingScheduler
from . import config
from .crawl import run_crawl
from .discover import find_channels, refresh_videos, run_discover
from .quota import QuotaLedger
from .replay import run_replay
from .sinks import Sink
from .store import connect
from .youtube import YouTube

log = logging.getLogger("main")


def ctx(cfg):
    conn = connect(config.DB_PATH)
    q = cfg["quota"]
    ledger = QuotaLedger(conn, q["daily_limit"], q["safety_ratio"], q["search_daily_cap"])
    return conn, ledger, YouTube(cfg["api_key"], ledger)


def job_discover(cfg):
    conn, ledger, yt = ctx(cfg)
    ledger.start_run(cfg.get("discover", {}).get("run_budget_units", 300))
    run_discover(cfg, yt, conn)


def job_snapshot(cfg):
    conn, ledger, yt = ctx(cfg)
    ledger.start_run(100)
    ids = [r[0] for r in conn.execute("SELECT video_id FROM videos WHERE status!='disabled'")]
    refresh_videos(yt, conn, ids)
    log.info("Đã chụp số liệu %d video", len(ids))


def job_crawl(cfg):
    conn, ledger, yt = ctx(cfg)
    ledger.start_run(cfg["crawl"]["run_budget_units"])
    run_crawl(cfg, yt, conn, Sink(cfg))


def cmd_status(cfg):
    conn = connect(config.DB_PATH)
    q = cfg["quota"]
    print("Hạn mức:", QuotaLedger(conn, q["daily_limit"], q["safety_ratio"], q["search_daily_cap"]).status())
    print("\nVideo theo mẫu máy / trạng thái (dùng để chốt mục còn mở #1: cần >= 20 video/mẫu):")
    for r in conn.execute("SELECT model,status,COUNT(*) n,SUM(comment_count) c FROM videos GROUP BY 1,2 ORDER BY 1,2"):
        print(f"  {r['model']:6} {r['status']:12} {r['n']:4} video  ~{r['c'] or 0} comment")
    print("\nĐã cào:", conn.execute("SELECT COUNT(*) FROM seen").fetchone()[0], "comment")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    p = argparse.ArgumentParser(prog="yt-crawler")
    sp = p.add_subparsers(dest="cmd", required=True)
    for n in ("discover", "crawl", "run", "status", "snapshot-videos"):
        sp.add_parser(n)
    fc = sp.add_parser("find-channels")
    fc.add_argument("queries", nargs="+")
    fc.add_argument("--calls", type=int, default=3)
    rp = sp.add_parser("replay")
    rp.add_argument("--speed", type=float, default=10)
    rp.add_argument("--model", choices=["main", "rival"])
    rp.add_argument("--since"); rp.add_argument("--until")
    rp.add_argument("--max-gap", type=float, default=2.0)
    rp.add_argument("--dry-run", action="store_true")
    rp.add_argument("--strip-stats", action="store_true", help="bỏ like_count/reply_count khi phát lại")
    a = p.parse_args()
    cfg = config.load()

    if a.cmd == "discover":
        job_discover(cfg)
    elif a.cmd == "crawl":
        job_crawl(cfg)
    elif a.cmd == "snapshot-videos":
        job_snapshot(cfg)
    elif a.cmd == "status":
        cmd_status(cfg)
    elif a.cmd == "find-channels":
        conn, ledger, yt = ctx(cfg)
        for title, cid, n in find_channels(cfg, yt, a.queries, a.calls):
            print(f"{n:3}  {title:40} {cid}")
    elif a.cmd == "replay":
        pt = lambda s: datetime.fromisoformat(s).replace(tzinfo=timezone.utc) if s else None
        run_replay(cfg, a.speed, a.model, pt(a.since), pt(a.until), a.max_gap, a.dry_run, a.strip_stats)
    elif a.cmd == "run":
        s = BlockingScheduler(timezone=cfg["timezone"])
        s.add_job(job_discover, "cron", hour=cfg["schedule"]["discover_cron_hour"], args=[cfg],
                  id="discover", max_instances=1, coalesce=True)
        s.add_job(job_snapshot, "cron", hour=cfg["schedule"]["discover_cron_hour"] + 1, args=[cfg],
                  id="snapshot", max_instances=1, coalesce=True)
        s.add_job(job_crawl, "interval", minutes=cfg["schedule"]["crawl_every_min"], args=[cfg],
                  id="crawl", max_instances=1, coalesce=True, next_run_time=datetime.now())
        log.info("Scheduler chạy. Ctrl+C để dừng.")
        try:
            job_discover(cfg)
            s.start()
        except (KeyboardInterrupt, SystemExit):
            pass


if __name__ == "__main__":
    main()

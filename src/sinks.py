"""Ghi ra 2 nơi: (1) archive JSONL (nguồn cho Replay, nguồn HDFS nếu môn yêu cầu),
(2) Kafka topic raw_comments khóa theo video_id (nếu có KAFKA_BOOTSTRAP)."""
import json
from datetime import datetime, timezone
from .config import RAW_DIR
from .schema import COMMENT_FIELDS, check


def utcnow_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def make_producer(bootstrap):
    from confluent_kafka import Producer
    return Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True,
                     "acks": "all", "linger.ms": 50, "compression.type": "lz4"})


class Sink:
    def __init__(self, cfg):
        RAW_DIR.mkdir(parents=True, exist_ok=True)
        self.topic = cfg["kafka"]["topic"]
        self.producer = make_producer(cfg["kafka"]["bootstrap"]) if cfg["kafka"]["bootstrap"] else None

    def write(self, recs):
        if not recs:
            return
        now = utcnow_iso()
        for r in recs:
            check(r, COMMENT_FIELDS)
            r["ingested_at"] = now
        path = RAW_DIR / f"comments-{now[:10]}.jsonl"
        with open(path, "a", encoding="utf-8") as f:
            for r in recs:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
        if self.producer:
            errors = []
            for r in recs:
                self.producer.produce(self.topic, key=r["video_id"],
                                      value=json.dumps(r, ensure_ascii=False).encode("utf-8"),
                                      on_delivery=lambda err, m: err and errors.append(err))
                self.producer.poll(0)
            self.producer.flush(30)
            if errors:
                raise RuntimeError(f"Kafka lỗi {len(errors)} message, ví dụ: {errors[0]}")

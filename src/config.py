import os
from pathlib import Path
import yaml
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")
DB_PATH = ROOT / "data" / "state.db"
RAW_DIR = ROOT / "data" / "raw"


def load(path=None):
    with open(path or ROOT / "config" / "config.yaml", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    cfg["api_key"] = os.environ.get("YOUTUBE_API_KEY", "")
    cfg["kafka"] = {
        "bootstrap": os.environ.get("KAFKA_BOOTSTRAP", ""),
        "topic": os.environ.get("KAFKA_TOPIC", "raw_comments"),
    }
    cfg["salt"] = os.environ.get("AUTHOR_SALT", "")
    return cfg

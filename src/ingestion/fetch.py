import os
import requests
import boto3
import json
import logging
from datetime import datetime, timezone
#if testing in local env
# from dotenv import load_dotenv 

# loads .env locally, ignored in Lambda (uses env vars instead)
# load_dotenv()

# ── config ────────────────────────────────────────────────
BUCKET = os.environ["BUCKET_NAME"]
REGION = os.environ["REGION_NAME"]
BASE_URL = "https://www.thesportsdb.com/api/v1/json/3/eventspastleague.php"

LEAGUES = {
    "4328": "English Premier League",
    "4329": "German Bundesliga",
    "4330": "Italian Serie A",
    "4331": "French Ligue 1",
    "4332": "Spanish La Liga",
    "4334": "Dutch Eredivisie",
    "4335": "Portuguese Primeira Liga",
}

KEEP_COLUMNS = [
    "idEvent", "strEvent", "dateEvent", "strTime",
    "strLeague", "strSeason", "strVenue", "strCountry",
    "strHomeTeam", "strAwayTeam",
    "intHomeScore", "intAwayScore", "strStatus",
]

# ── logging ───────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


# ── fetch ─────────────────────────────────────────────────
def fetch_league(league_id: str, league_name: str) -> list[dict]:
    try:
        response = requests.get(BASE_URL, params={"id": league_id}, timeout=10)
        response.raise_for_status()
        events = response.json().get("events") or []
        cleaned = []
        for e in events:
            row = {col: e.get(col) for col in KEEP_COLUMNS}
            row["league_name"] = league_name
            row["ingested_at"] = datetime.now(timezone.utc).isoformat()
            cleaned.append(row)
        logger.info(f"{league_name}: {len(cleaned)} matches fetched")
        return cleaned
    except requests.RequestException as ex:
        logger.error(f"Failed to fetch {league_name}: {ex}")
        return []


def fetch_all() -> list[dict]:
    all_events = []
    for league_id, league_name in LEAGUES.items():
        all_events.extend(fetch_league(league_id, league_name))
    logger.info(f"Total matches fetched: {len(all_events)}")
    return all_events


# ── s3 ────────────────────────────────────────────────────
def save_to_s3(events: list[dict]) -> str:
    s3 = boto3.client("s3", region_name=REGION)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    key = f"raw/matches/date={today}/matches.json"
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(events, indent=2),
        ContentType="application/json",
        Metadata={
            "record_count": str(len(events)),
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        }
    )
    logger.info(f"Saved to s3://{BUCKET}/{key}")
    return key


# ── lambda handler ─────────────────────────────────────────
def handler(event=None, context=None):
    try:
        events = fetch_all()
        if not events:
            logger.warning("No data fetched — skipping S3 upload")
            return {"status": "no_data"}
        key = save_to_s3(events)
        return {"status": "success", "s3_key": key, "record_count": len(events)}
    except Exception as e:
        logger.exception(f"Pipeline failed: {e}")
        raise


if __name__ == "__main__":
    result = handler()
    logger.info(result)
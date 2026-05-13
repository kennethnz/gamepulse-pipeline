import sys
import os
import boto3
import logging
from datetime import datetime, timezone
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

# ── logging ───────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# ── args & context ─────────────────────────────────────────
args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET_NAME", "REGION_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ── config ─────────────────────────────────────────────────
BUCKET = args["BUCKET_NAME"]
REGION = args["REGION_NAME"]
today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
RAW_PATH = f"s3://{BUCKET}/raw/matches/date={today}/matches.json"
CURATED_PATH = f"s3://{BUCKET}/curated/matches/"

logger.info(f"Reading from: {RAW_PATH}")

# ── verify file exists ─────────────────────────────────────
s3 = boto3.client("s3", region_name=REGION)
try:
    s3.head_object(Bucket=BUCKET, Key=f"raw/matches/date={today}/matches.json")
    logger.info("Raw file found in S3")
except Exception as e:
    logger.error(f"Raw file not found in S3: {e}")
    job.commit()
    sys.exit(0)

# ── read ───────────────────────────────────────────────────
df = spark.read.option("multiline", "true").json(RAW_PATH)
logger.info(f"Rows read: {df.count()}")

# ── transform ──────────────────────────────────────────────
df_clean = (
    df
    .withColumn("home_score", F.col("intHomeScore").cast(IntegerType()))
    .withColumn("away_score", F.col("intAwayScore").cast(IntegerType()))
    .withColumn("winner",
        F.when(F.col("home_score") > F.col("away_score"), F.col("strHomeTeam"))
         .when(F.col("away_score") > F.col("home_score"), F.col("strAwayTeam"))
         .otherwise("draw"))
    .withColumn("total_goals", F.col("home_score") + F.col("away_score"))
    .withColumnRenamed("strEvent", "match_name")
    .withColumnRenamed("dateEvent", "match_date")
    .withColumnRenamed("strLeague", "league")
    .withColumnRenamed("strSeason", "season")
    .withColumnRenamed("strVenue", "venue")
    .withColumnRenamed("strCountry", "country")
    .withColumnRenamed("strHomeTeam", "home_team")
    .withColumnRenamed("strAwayTeam", "away_team")
    .withColumnRenamed("strStatus", "status")
    .select(
        "idEvent", "match_name", "match_date", "league",
        "league_name", "season", "venue", "country",
        "home_team", "away_team", "home_score", "away_score",
        "winner", "total_goals", "status", "ingested_at"
    )
    .withColumn("processed_date", F.lit(today))
)

logger.info(f"Clean rows: {df_clean.count()}")

# ── write curated ──────────────────────────────────────────
df_clean.write \
    .mode("overwrite") \
    .partitionBy("processed_date") \
    .parquet(CURATED_PATH)

logger.info(f"Written to: {CURATED_PATH}")
job.commit()
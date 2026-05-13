# 🏆 GamePulse Analytics Pipeline

An end-to-end automated sports analytics pipeline built on AWS. Ingests live match data daily from a REST API, transforms it through a medallion architecture, and serves it via a Power BI dashboard.

![Pipeline](https://img.shields.io/badge/AWS-Lambda%20%7C%20S3%20%7C%20Glue%20%7C%20Athena-orange?logo=amazonaws)
![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)
![PySpark](https://img.shields.io/badge/PySpark-Glue-red?logo=apachespark)
![CI/CD](https://img.shields.io/badge/CI%2FCD-GitHub%20Actions-black?logo=githubactions)
![Dashboard](https://img.shields.io/badge/Dashboard-Power%20BI-yellow?logo=powerbi)

---

## Architecture

```
TheSportsDB API (7 leagues)
    → AWS Lambda (Python)
        → S3 raw/          ← partitioned by date
            → AWS Glue (PySpark)
                → S3 curated/  ← parquet, partitioned by processed_date
                    → AWS Athena
                        → Power BI Dashboard
```

---

## Pipeline Stages

| Stage | Tool | Description |
|---|---|---|
| Ingestion | AWS Lambda | Fetches match data from API across 7 leagues |
| Storage | AWS S3 | Data lake with raw and curated zones |
| Transform | AWS Glue (PySpark) | Cleans, casts, enriches and writes parquet |
| Query layer | AWS Athena | External table on top of S3 curated parquet |
| Orchestration | AWS EventBridge | Triggers Lambda daily at 11:30am IST |
| CI/CD | GitHub Actions | Auto deploys Lambda + Glue script on push to main |
| Dashboard | Power BI | Goals, results, win distribution, totals |

---

## Data Flow

```
raw/matches/date=YYYY-MM-DD/matches.json     ← Lambda drops here daily
curated/matches/processed_date=YYYY-MM-DD/   ← Glue writes parquet here
athena-results/                              ← Athena query results
scripts/clean.py                             ← Glue script (auto deployed)
```

---

## Dashboard

Built in Power BI Desktop connected to Athena via ODBC:

- **Goals by League** — bar chart showing total goals per league
- **Match Results** — table with match name, scores and winner
- **Win Distribution** — donut chart of home wins, away wins, draws
- **Total Goals** — card showing overall goals scored

---

## Security

Every service has its own IAM role or user with least privilege:

| Service | Permissions |
|---|---|
| Lambda | S3 PutObject on `raw/*` only |
| Glue | S3 GetObject + PutObject on full bucket |
| GitHub Actions | Lambda UpdateFunctionCode + S3 PutObject on `scripts/*` only |
| Power BI | Athena read + S3 GetObject only |

GitHub Actions uses **IAM OIDC** — no AWS keys stored anywhere.

---

## CI/CD

Every push to `main`:
```
checkout code
    → configure AWS via OIDC (no keys)
        → install dependencies
            → zip and deploy to Lambda
                → upload Glue script to S3
```

---

## Project Structure

```
gamepulse-pipeline/
├── .github/
│   └── workflows/
│       └── deploy.yml        # GitHub Actions CI/CD
├── src/
│   ├── ingestion/
│   │   └── fetch.py          # Lambda function
│   └── transform/
│       └── clean.py          # Glue PySpark job
├── .gitignore
└── README.md

---

## Tech Stack

```
Language        Python 3.11, PySpark, SQL
Ingestion       AWS Lambda, EventBridge
Storage         AWS S3
Transform       AWS Glue (Spark 3.5)
Query Layer     AWS Athena + Glue Data Catalog
CI/CD           GitHub Actions + IAM OIDC
Dashboard       Power BI Desktop (ODBC → Athena)
```

---

## How to Run Locally

```bash
# clone the repo
git clone https://github.com/YOUR_USERNAME/gamepulse-pipeline.git
cd gamepulse-pipeline

# install dependencies
pip install requests boto3

# configure AWS credentials
aws configure

# run ingestion locally
python src/ingestion/fetch.py
```

---

## Lessons Learned

- Athena external tables query S3 directly — no data loading needed
- Partitioning S3 data by date reduces Athena scan costs significantly
- IAM OIDC is safer than storing AWS keys in GitHub secrets
- Glue requires `df.count()` or another action to force Spark lazy evaluation
- Least privilege IAM policies should be tested with the restricted user, not admin

---

## What I Would Add Next

- [ ] Data quality checks with Great Expectations
- [ ] dbt models on top of Athena (staging / intermediate / mart)
- [ ] CloudWatch alarms + Slack notifications on failure
- [ ] Airflow orchestration replacing EventBridge
- [ ] Streaming ingestion with Kinesis
- [ ] React dashboard via API Gateway + Lambda

---

## Author

Built by Kennethnz as part of a hands-on data engineering learning project.



# DataFlow Automation Suite

> Automated ETL pipeline that processes **2M+ records/day** from 10+ data sources — no manual work.

## The Problem
A client was spending 3 hours every day manually moving data between systems — downloading CSVs, cleaning them up, uploading to the warehouse. I built this so it just runs on its own.

## What It Does
- Pulls from 10+ sources (APIs, databases, S3, SFTP, Excel files)
- Detects and fixes schema mismatches automatically
- Flags anomalies and sends Slack/email alerts
- Lands clean data into the analytics warehouse every morning at 6 AM

## Stack
```
Python 3.11 | Pandas | Celery | Redis | SQLAlchemy | Airflow | PostgreSQL
```

## Results
- Eliminated 3 hours/day of manual work
- 99.3% data quality score (up from ~71%)
- Zero failed pipeline runs in 6 months of production

## Contact
Built by **Shebin S Illikkal** — [Shebinsillikkl@gmail.com](mailto:Shebinsillikkl@gmail.com)

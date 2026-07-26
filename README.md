# Automated Weekly Port Terminal Operations Report

An end-to-end automation pipeline that replaces a manual weekly reporting
process. Pulls raw vessel-call data, computes key terminal KPIs (TEU
throughput, berth utilization, dwell time), generates a formatted Excel
report, and emails it to stakeholders — fully automated on a weekly schedule.

**Impact:** eliminates ~X hours/week of manual data pulling and spreadsheet
work by automating extraction, aggregation, and report distribution.

## Why this project

Port terminals track vessel calls, container throughput, and berth
utilization constantly — but turning that raw operational data into a
weekly management report is still, in a lot of terminals, a manual process:
someone exports a CSV, opens Excel, builds pivot tables, and emails it out.
This project automates that entire chain.

## Architecture

```
Raw data (CSV / TOS export)
        │
        ▼
   EXTRACT  ── validate the source file exists and is readable
        │
        ▼
  TRANSFORM ── filter to the reporting window, compute KPIs
        │        (vessel calls, TEU moves, berth utilization, dwell time)
        ▼
    LOAD    ── write a formatted Excel report
        │
        ▼
   NOTIFY   ── email the report to stakeholders
```

This logic is implemented twice, intentionally, to show the progression:

| File | Orchestration | Use case |
|---|---|---|
| `weekly_report.py` + `run_weekly_report.sh` + `cron` | OS-level scheduling | Simple, single-machine automation |
| `weekly_terminal_report_dag.py` | Apache Airflow | Production-grade: retries, failure alerting, history/UI, backfills |

## Key features

- **ETL structure** — clean separation of extract/transform/load, so each
  stage can be tested and modified independently
- **Error handling & logging** — failures are logged clearly (`pipeline.log`)
  rather than failing silently; a missing input file raises immediately
  instead of producing a blank report
- **Automated scheduling** — both a `cron`-based version and an Airflow DAG
  (`schedule="0 6 * * 1"` — every Monday at 6 AM)
- **Retry logic & alerting** — the Airflow DAG retries transient failures
  automatically and fires an alert callback only once retries are exhausted
- **Email delivery** — reports are emailed as attachments via SMTP, with
  credentials read from environment variables (never hardcoded) and a
  `dry_run` mode for safe testing

## Setup

```bash
pip install -r requirements.txt

# Generate sample data (or point INPUT_FILE at your own TOS export)
python3 generate_sample_data.py

# Run the pipeline once, manually
python3 weekly_report.py
```

### Enable email delivery

Set these environment variables (e.g. in a `.env` file, not committed):

```bash
export SMTP_HOST=smtp.gmail.com
export SMTP_PORT=587
export SMTP_USER=your_email@example.com
export SMTP_PASSWORD=your_app_password   # use an app password, not your real password
export REPORT_RECIPIENTS=manager@example.com,ops-team@example.com
```

Then set `dry_run=False` in `weekly_report.py`'s call to `send_report(...)`.

### Schedule it

**Option A — cron (simple, single machine):**
```bash
chmod +x run_weekly_report.sh
crontab -e
# add: 0 6 * * 1 /path/to/run_weekly_report.sh
```

**Option B — Airflow (production):**
```bash
export AIRFLOW_HOME=~/airflow
airflow db migrate
cp weekly_terminal_report_dag.py $AIRFLOW_HOME/dags/
airflow standalone   # starts the scheduler + webserver UI
```

## Sample output

See `output/weekly_terminal_report_<date>.xlsx` for an example generated
report, including KPI summary, per-berth performance, per-shipping-line
performance, and per-operation-type breakdown.

## Tech stack

Python · pandas · openpyxl · Apache Airflow · cron · SMTP

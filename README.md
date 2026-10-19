# Automated Weekly Port Terminal Operations Report

An end-to-end automation pipeline that replaces a manual weekly reporting
process. Pulls raw vessel-call data, computes key terminal KPIs (TEU
throughput, berth utilization, dwell time), generates a formatted Excel
report, and supports SMTP delivery when explicitly enabled. Email delivery is disabled
by default. The Airflow example writes JSON and logs notifications only.

**Goal:** reduce manual data pulling and spreadsheet work. Time savings have
not been measured.

## Why this project

Port terminals track vessel calls, container throughput, and berth
utilization constantly — but turning that raw operational data into a
weekly management report is still, in a lot of terminals, a manual process:
someone exports a CSV, opens Excel, builds pivot tables, and emails it out.
The standalone pipeline implements that chain, with email delivery opt-in.

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

Two examples demonstrate different orchestration approaches and outputs:

| File | Orchestration | Use case |
|---|---|---|
| `weekly_report.py` + `run_weekly_report.sh` + `cron` | OS-level scheduling | Simple, single-machine automation |
| `weekly_terminal_report_dag.py` | Apache Airflow | JSON demo: retries, log-only failure callback, history/UI |

## Key features

- **ETL structure** — clean separation of extract/transform/load, so each
  stage can be tested and modified independently
- **Error handling & logging** — failures are logged clearly (`pipeline.log`)
  rather than failing silently; a missing input file raises immediately
  instead of producing a blank report
- **Automated scheduling** — both a `cron`-based version and an Airflow DAG
  (`schedule="0 6 * * 1"` — every Monday at 6 AM)
- **Retry logic & alerting** — the Airflow DAG retries transient failures
  automatically and invokes a log-only callback once retries are exhausted;
  it does not send email or Slack alerts
- **Email delivery (standalone only, opt-in)** — `notifier.py` supports SMTP attachments, with
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

Export these variables in the process running the standalone pipeline. A `.env`
file is not loaded automatically; cron also does not inherit your interactive shell:

```bash
export SMTP_HOST=smtp.gmail.com
export SMTP_PORT=587
export SMTP_USER=your_email@example.com
export SMTP_PASSWORD=your_app_password   # use an app password, not your real password
export REPORT_RECIPIENTS=manager@example.com,ops-team@example.com
```

Then set `dry_run=False` in `weekly_report.py`'s call to `send_report(...)`.
With the default `dry_run=True`, no SMTP connection is opened. Without SMTP
configuration, notification is skipped and Excel generation still succeeds.

The DAG does not call `notifier.py`: `notify` prints a report path and
`alert_on_failure` prints failure details. Neither sends email or Slack messages.
Its output is `output/airflow_report_<date>.json`, with fewer KPIs than Excel.

### Schedule it

**Option A — cron (simple, single machine):**
```bash
chmod +x run_weekly_report.sh
crontab -e
# add (replace both absolute paths with your checkout and virtualenv paths):
# 0 6 * * 1 PYTHON_BIN=/absolute/path/.venv/bin/python /absolute/path/terminal_pipeline/run_weekly_report.sh
```

The wrapper changes into its own directory before running, so CSV input,
Excel output and `pipeline.log` are relative to the checkout. It propagates
Python's exit status. Generate sample data before scheduling.

**Option B — Airflow (demonstration):**
```bash
# Run from the repository root; Python 3.11 is a suitable choice for Airflow 2.10.4.
export TERMINAL_PIPELINE_PROJECT_DIR="$(pwd)"
export AIRFLOW_HOME=~/airflow
mkdir -p "$AIRFLOW_HOME/dags"
airflow db migrate
cp weekly_terminal_report_dag.py "$AIRFLOW_HOME/dags/"
airflow standalone   # starts the scheduler + webserver UI
```

`TERMINAL_PIPELINE_PROJECT_DIR` must be an absolute path. When unset, the DAG
uses the directory containing the DAG file. If you copy only the DAG into
Airflow's DAG folder, set this variable to the project checkout before starting
Airflow. Set it consistently on the scheduler and every worker; the CSV and
writable output directory must be shared at the same path for distributed
executors. This example passes filesystem paths through XCom, not file contents.
The Monday 06:00 schedule uses Airflow's configured default timezone.

For a fresh Airflow install, use the official constraints for your Python version:
```bash
python3.11 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.10.4/constraints-3.11.txt
```

## Validation

```bash
python3 -m unittest discover -s tests -v
bash -n run_weekly_report.sh
python3 generate_sample_data.py
bash run_weekly_report.sh
# With Airflow installed and configured as above:
airflow dags list-import-errors
airflow dags test weekly_terminal_report 2026-01-05
```

The unit tests use a small Airflow decorator stub to exercise configuration
and task bodies without installing a scheduler. The Airflow commands separately
check real DAG parsing and execution. No notification tests send email.

## Sample output

See `output/weekly_terminal_report_<date>.xlsx` for an example generated
report, including KPI summary, per-berth performance, per-shipping-line
performance, and per-operation-type breakdown.

## Tech stack

Python · pandas · openpyxl · Apache Airflow · cron · SMTP

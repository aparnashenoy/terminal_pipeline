"""
weekly_terminal_report_dag.py

Airflow DAG version of our weekly terminal operations report.

This wraps the SAME extract/transform/load logic from weekly_report.py,
but now Airflow handles: scheduling, retries, dependency ordering,
logging/history (visible in the UI), and backfills.

Key concepts to notice vs. the cron version:
  - Each pipeline stage becomes its own TASK (not just a function call)
  - Airflow tracks each task's success/failure independently
  - `retries` + `retry_delay` replace manual error handling for transient failures
  - The `schedule` param replaces the crontab line - same cron syntax!
  - Data is passed between tasks via return values (Airflow's "XCom" under the hood)
"""

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task

# When copied to Airflow's DAG folder, point this at the project/data checkout.
# Configure the same absolute path on the scheduler and every worker.
_project_dir = os.environ.get("TERMINAL_PIPELINE_PROJECT_DIR")
PROJECT_DIR = (
    Path(_project_dir).expanduser() if _project_dir else Path(__file__).resolve().parent
)
if not PROJECT_DIR.is_absolute():
    raise ValueError("TERMINAL_PIPELINE_PROJECT_DIR must be an absolute path")
PROJECT_DIR = PROJECT_DIR.resolve()


def alert_on_failure(context):
    """
    Airflow calls this automatically if any task exhausts its retries and
    still fails. In production this would call notifier.py's send_report-style
    logic (or post to Slack) - here we log clearly so the pattern is visible.

    `context` is a dict Airflow injects with rich failure detail: which task,
    which DAG run, the exception, log URL, etc.
    """
    task_id = context["task_instance"].task_id
    dag_id = context["dag"].dag_id
    exception = context.get("exception")
    print(f"LOG-ONLY ALERT (no email/Slack sent): Task '{task_id}' in DAG '{dag_id}' failed after all retries. Error: {exception}")
    # Production version: notifier.send_failure_alert(task_id, dag_id, exception)


default_args = {
    "owner": "analytics-team",
    "retries": 2,                       # retry a failed task twice...
    "retry_delay": timedelta(minutes=5),  # ...waiting 5 min between attempts
    "on_failure_callback": alert_on_failure,  # fires after retries are exhausted
}


@dag(
    dag_id="weekly_terminal_report",
    description="Automated weekly port terminal operations report",
    schedule="0 6 * * 1",          # <-- same cron syntax as Step 3: every Monday 6 AM
    start_date=datetime(2026, 1, 1),
    catchup=False,                  # don't backfill historical runs on first deploy
    default_args=default_args,
    tags=["terminal-ops", "reporting"],
)
def weekly_terminal_report():

    @task
    def extract() -> str:
        """Read the raw CSV and return its path (small, so passing the path is fine;
        for big data you'd pass a reference like an S3 key, not the data itself)."""
        import pandas as pd

        input_file = PROJECT_DIR / "raw_terminal_data.csv"
        if not input_file.exists():
            raise FileNotFoundError(f"Missing {input_file}")

        df = pd.read_csv(input_file, parse_dates=["date"])
        print(f"Extracted {len(df)} rows")
        return str(input_file)

    @task
    def transform(input_path: str) -> dict:
        """Filter to the last 7 days and compute KPIs. Returns a dict of
        JSON-serializable results, since XCom (Airflow's inter-task data
        passing) stores data as JSON under the hood."""
        import pandas as pd
        from datetime import timedelta as td

        df = pd.read_csv(input_path, parse_dates=["date"])
        latest_date = df["date"].max()
        window_start = latest_date - td(days=6)
        week_df = df[(df["date"] >= window_start) & (df["date"] <= latest_date)]

        summary = {
            "window_start": str(window_start.date()),
            "window_end": str(latest_date.date()),
            "total_vessel_calls": int(week_df["vessel_call_id"].nunique()),
            "total_teu_moves": int(week_df["teu_moves"].sum()),
            "avg_dwell_time_hours": round(float(week_df["avg_dwell_time_hours"].mean()), 1),
        }

        by_berth = (
            week_df.groupby("berth")["teu_moves"].sum().sort_values(ascending=False).to_dict()
        )

        print(f"Transformed: {summary}")
        return {"summary": summary, "by_berth": by_berth}

    @task
    def load(results: dict) -> str:
        """Write the final report. In production this might upload to S3,
        email stakeholders, or post to Slack instead of / in addition to
        writing a local file."""
        import json

        output_dir = PROJECT_DIR / "output"
        output_dir.mkdir(exist_ok=True)
        out_path = output_dir / f"airflow_report_{results['summary']['window_end']}.json"

        with open(out_path, "w") as f:
            json.dump(results, f, indent=2)

        print(f"Report written to {out_path}")
        return str(out_path)

    @task
    def notify(report_path: str):
        """Placeholder for the 'last mile' - in production this would send
        a Slack message or email. Demonstrates a downstream dependency."""
        print(f"Report ready at {report_path}. (Would email/Slack this in production.)")

    # --- Task dependency chain ---
    # This is Airflow's core idea: define the ORDER, and Airflow ensures
    # each task only runs after its upstream task succeeds.
    raw_path = extract()
    results = transform(raw_path)
    report_path = load(results)
    notify(report_path)


weekly_terminal_report()

"""
notifier.py

Handles email delivery for the weekly terminal report pipeline.

Design choice: credentials are read from environment variables, NEVER
hardcoded. This is the standard practice for any pipeline - whether it
runs under cron, Airflow, or a cloud function.

Required environment variables:
    SMTP_HOST       e.g. smtp.gmail.com
    SMTP_PORT       e.g. 587
    SMTP_USER       the sending email address
    SMTP_PASSWORD   an app password (NOT your regular account password)
    REPORT_RECIPIENTS   comma-separated list, e.g. "ops@company.com,manager@company.com"
"""

import logging
import os
import smtplib
from email.message import EmailMessage
from pathlib import Path

log = logging.getLogger(__name__)


class EmailConfigError(Exception):
    """Raised when required SMTP environment variables are missing."""
    pass


def _load_config() -> dict:
    required = ["SMTP_HOST", "SMTP_PORT", "SMTP_USER", "SMTP_PASSWORD", "REPORT_RECIPIENTS"]
    missing = [var for var in required if not os.environ.get(var)]
    if missing:
        raise EmailConfigError(
            f"Missing required environment variables: {', '.join(missing)}. "
            f"Set these before running the pipeline with email delivery enabled."
        )

    return {
        "host": os.environ["SMTP_HOST"],
        "port": int(os.environ["SMTP_PORT"]),
        "user": os.environ["SMTP_USER"],
        "password": os.environ["SMTP_PASSWORD"],
        "recipients": [r.strip() for r in os.environ["REPORT_RECIPIENTS"].split(",")],
    }


def send_report(report_path: Path, summary: dict, dry_run: bool = False) -> bool:
    """
    Emails the report file as an attachment.

    dry_run=True builds the message and logs what WOULD be sent, without
    opening a network connection. Useful for testing pipelines in sandboxes
    or CI environments that don't have real SMTP credentials configured.
    """
    try:
        config = _load_config()
    except EmailConfigError as e:
        log.warning(f"Email not sent - {e}")
        return False

    msg = EmailMessage()
    msg["Subject"] = f"Weekly Terminal Operations Report - {summary.get('window_end', 'N/A')}"
    msg["From"] = config["user"]
    msg["To"] = ", ".join(config["recipients"])

    body_lines = [
        "Automated weekly terminal operations report attached.",
        "",
        "Summary:",
    ]
    for key, value in summary.items():
        body_lines.append(f"  - {key.replace('_', ' ').title()}: {value}")

    msg.set_content("\n".join(body_lines))

    with open(report_path, "rb") as f:
        msg.add_attachment(
            f.read(),
            maintype="application",
            subtype="octet-stream",
            filename=report_path.name,
        )

    if dry_run:
        log.info(
            f"[DRY RUN] Would send email to {config['recipients']} "
            f"via {config['host']}:{config['port']} with attachment {report_path.name}"
        )
        return True

    try:
        with smtplib.SMTP(config["host"], config["port"], timeout=30) as server:
            server.starttls()
            server.login(config["user"], config["password"])
            server.send_message(msg)
        log.info(f"Email sent to {config['recipients']}")
        return True
    except Exception:
        log.exception("Failed to send email")
        return False

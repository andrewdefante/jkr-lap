"""
Email utility for MiaCorp Analytics daily reports.
Uses Gmail SMTP with app password.
"""
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import date


def send_email(subject: str, body_html: str, body_text: str = None) -> bool:
    """Send an email via Gmail SMTP. Returns True on success."""
    smtp_host = os.environ.get("EMAIL_SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.environ.get("EMAIL_SMTP_PORT", 587))
    smtp_user = os.environ.get("EMAIL_SMTP_USER")
    smtp_pass = os.environ.get("EMAIL_SMTP_PASSWORD")
    email_from = os.environ.get("EMAIL_FROM")
    email_to = os.environ.get("EMAIL_TO")

    if not all([smtp_user, smtp_pass, email_from, email_to]):
        print("ERROR: Email env vars not set")
        return False

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = f"MiaCorp Analytics <{email_from}>"
    msg["To"] = email_to

    if body_text:
        msg.attach(MIMEText(body_text, "plain"))
    msg.attach(MIMEText(body_html, "html"))

    try:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.ehlo()
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.sendmail(email_from, email_to, msg.as_string())
        print(f"Email sent: {subject}")
        return True
    except Exception as e:
        print(f"Email ERROR: {e}")
        return False


def send_integrity_email(pipeline_log: list, pa_validation: dict) -> bool:
    """Send daily data integrity report."""
    today = date.today().strftime("%A, %B %d, %Y")
    subject = f"MiaCorp — Data Integrity Report · {date.today()}"

    # Build status rows
    rows = ""
    for step in pipeline_log:
        status = "✅" if step.get("success") else "❌"
        duration = f"{step.get('duration', 0):.0f}s"
        detail = step.get("detail", "")
        rows += f"""
        <tr>
            <td style="padding:6px 12px;font-family:Courier New">{status} {step['name']}</td>
            <td style="padding:6px 12px;color:#666">{duration}</td>
            <td style="padding:6px 12px;color:#444;font-size:12px">{detail}</td>
        </tr>"""

    # PA validation section
    pa = pa_validation or {}
    pa_status = "✅ OK" if pa.get("diff_pct", 0) < 2.0 else "⚠️ MISMATCH"
    pa_color = "#2d7d46" if pa.get("diff_pct", 0) < 2.0 else "#c0392b"

    # Mismatch table
    mismatch_rows = ""
    for m in pa.get("mismatches", []):
        mismatch_rows += f"""
        <tr>
            <td style="padding:4px 8px">{m['name']}</td>
            <td style="padding:4px 8px;text-align:right">{m['bbref_pa']}</td>
            <td style="padding:4px 8px;text-align:right">{m['our_pa']}</td>
            <td style="padding:4px 8px;text-align:right;color:#c0392b">{m['gap']}</td>
        </tr>"""

    body_html = f"""
    <html><body style="font-family:Arial,sans-serif;max-width:700px;margin:0 auto;padding:20px">
    <div style="background:#1a3a5c;padding:16px 24px;margin-bottom:24px">
        <h2 style="color:white;margin:0;font-family:Courier New;letter-spacing:0.1em">
            MIACORP ANALYTICS
        </h2>
        <div style="color:#aac;font-size:12px;font-family:Courier New">
            DATA INTEGRITY REPORT · {today}
        </div>
    </div>

    <h3 style="color:#1a3a5c;border-bottom:2px solid #1a3a5c;padding-bottom:6px">
        Pipeline Run Log
    </h3>
    <table style="width:100%;border-collapse:collapse;font-size:13px">
        <tr style="background:#f5f5f5">
            <th style="padding:6px 12px;text-align:left">Step</th>
            <th style="padding:6px 12px;text-align:left">Time</th>
            <th style="padding:6px 12px;text-align:left">Detail</th>
        </tr>
        {rows}
    </table>

    <h3 style="color:#1a3a5c;border-bottom:2px solid #1a3a5c;padding-bottom:6px;margin-top:24px">
        PA Validation
    </h3>
    <table style="width:100%;border-collapse:collapse;font-size:13px">
        <tr>
            <td style="padding:8px 12px">Status</td>
            <td style="padding:8px 12px;font-weight:bold;color:{pa_color}">{pa_status}</td>
        </tr>
        <tr style="background:#f5f5f5">
            <td style="padding:8px 12px">BBref Season PA</td>
            <td style="padding:8px 12px">{pa.get('sum_bbref_pa', 'N/A'):,}</td>
        </tr>
        <tr>
            <td style="padding:8px 12px">Our Season PA</td>
            <td style="padding:8px 12px">{pa.get('sum_our_pa', 'N/A'):,}</td>
        </tr>
        <tr style="background:#f5f5f5">
            <td style="padding:8px 12px">Difference</td>
            <td style="padding:8px 12px">{pa.get('diff_pct', 0):.1f}%</td>
        </tr>
    </table>
    {f'''
    <h4 style="color:#c0392b;margin-top:16px">Individual Mismatches</h4>
    <table style="width:100%;border-collapse:collapse;font-size:12px">
        <tr style="background:#fdecea">
            <th style="padding:4px 8px;text-align:left">Player</th>
            <th style="padding:4px 8px;text-align:right">BBref PA</th>
            <th style="padding:4px 8px;text-align:right">Our PA</th>
            <th style="padding:4px 8px;text-align:right">Gap</th>
        </tr>
        {mismatch_rows}
    </table>''' if mismatch_rows else ''}

    <div style="margin-top:32px;padding-top:16px;border-top:1px solid #ddd;
                color:#888;font-size:11px;font-family:Courier New">
        MiaCorp Analytics · Automated Report · {date.today()}
    </div>
    </body></html>
    """

    return send_email(subject, body_html)


def send_pipeline_integrity_alert(fixed_games: list, error_games: list, pa_discrepancies: list) -> bool:
    """Send an alert email when the nightly integrity check finds missing games or PA mismatches."""
    today = date.today().strftime("%A, %B %d, %Y")
    subject = f"⚠️ MiaCorp Data Integrity Alert — {date.today()}"

    def _game_rows(games, color):
        rows = ""
        for g in games:
            rows += f"""
            <tr>
                <td style="padding:4px 8px">{g['game_date']}</td>
                <td style="padding:4px 8px">{g['matchup']}</td>
                <td style="padding:4px 8px">{g['game_pk']}</td>
                <td style="padding:4px 8px;color:{color}">{g['reason']}</td>
            </tr>"""
        return rows

    fixed_rows = _game_rows(fixed_games, "#2d7d46")
    error_rows = _game_rows(error_games, "#c0392b")

    pa_rows = ""
    for d in pa_discrepancies:
        pa_rows += f"""
        <tr>
            <td style="padding:4px 8px">{d['game_date']}</td>
            <td style="padding:4px 8px">{d['game_pk']}</td>
            <td style="padding:4px 8px;text-align:right">{d['our_pa']}</td>
            <td style="padding:4px 8px;text-align:right">{d['api_pa']}</td>
            <td style="padding:4px 8px;text-align:right;color:#c0392b">{d['diff']}</td>
        </tr>"""

    body_html = f"""
    <html><body style="font-family:Arial,sans-serif;max-width:700px;margin:0 auto;padding:20px">
    <div style="background:#c0392b;padding:16px 24px;margin-bottom:24px">
        <h2 style="color:white;margin:0;font-family:Courier New;letter-spacing:0.1em">
            MIACORP ANALYTICS
        </h2>
        <div style="color:#fdd;font-size:12px;font-family:Courier New">
            DATA INTEGRITY ALERT · {today}
        </div>
    </div>

    {f'''
    <h3 style="color:#1a3a5c;border-bottom:2px solid #1a3a5c;padding-bottom:6px">
        Games Fixed ({len(fixed_games)})
    </h3>
    <table style="width:100%;border-collapse:collapse;font-size:13px;margin-bottom:24px">
        <tr style="background:#f5f5f5">
            <th style="padding:6px 12px;text-align:left">Date</th>
            <th style="padding:6px 12px;text-align:left">Matchup</th>
            <th style="padding:6px 12px;text-align:left">Game PK</th>
            <th style="padding:6px 12px;text-align:left">Reason</th>
        </tr>
        {fixed_rows}
    </table>''' if fixed_games else ''}

    {f'''
    <h3 style="color:#c0392b;border-bottom:2px solid #c0392b;padding-bottom:6px">
        Games Failed to Fix ({len(error_games)})
    </h3>
    <table style="width:100%;border-collapse:collapse;font-size:13px;margin-bottom:24px">
        <tr style="background:#fdecea">
            <th style="padding:6px 12px;text-align:left">Date</th>
            <th style="padding:6px 12px;text-align:left">Matchup</th>
            <th style="padding:6px 12px;text-align:left">Game PK</th>
            <th style="padding:6px 12px;text-align:left">Reason</th>
        </tr>
        {error_rows}
    </table>''' if error_games else ''}

    {f'''
    <h3 style="color:#1a3a5c;border-bottom:2px solid #1a3a5c;padding-bottom:6px">
        PA Count Mismatches ({len(pa_discrepancies)})
    </h3>
    <table style="width:100%;border-collapse:collapse;font-size:13px">
        <tr style="background:#f5f5f5">
            <th style="padding:6px 12px;text-align:left">Date</th>
            <th style="padding:6px 12px;text-align:left">Game PK</th>
            <th style="padding:6px 12px;text-align:right">Our PA</th>
            <th style="padding:6px 12px;text-align:right">API PA</th>
            <th style="padding:6px 12px;text-align:right">Diff</th>
        </tr>
        {pa_rows}
    </table>''' if pa_discrepancies else ''}

    <div style="margin-top:32px;padding-top:16px;border-top:1px solid #ddd;
                color:#888;font-size:11px;font-family:Courier New">
        MiaCorp Analytics · Automated Report · {date.today()}
    </div>
    </body></html>
    """

    return send_email(subject, body_html)


def send_daily_brief_email(homepage_html: str) -> bool:
    """Send the daily morning brief as email."""
    today = date.today().strftime("%A, %B %d, %Y")
    subject = f"MiaCorp Morning Brief · {today}"

    # Wrap homepage in email-safe container
    body_html = f"""
    <html><body style="max-width:800px;margin:0 auto">
        {homepage_html}
    </body></html>
    """

    return send_email(subject, body_html)

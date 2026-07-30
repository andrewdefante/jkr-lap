"""
Analyze nginx access logs for miacorpdata.com
Filters out known bots, scanners, and your own IP.
Produces a summary report of real human candidates.

Usage:
    python3 scripts/analyze_traffic.py --log /path/to/access.log
    python3 scripts/analyze_traffic.py  # reads from stdin
"""
import argparse
import re
import sys
from collections import Counter
from datetime import datetime, date as date_cls

LOG_RE = re.compile(
    r'^(?P<ip>\S+) \S+ \S+ \[(?P<date>[^\]]+)\] '
    r'"(?P<method>\S+) (?P<path>\S+) (?P<protocol>[^"]+)" '
    r'(?P<status>\d+) (?P<bytes>\S+) '
    r'"(?P<referer>[^"]*)" "(?P<ua>[^"]*)"'
    r'(?: "(?P<xff>[^"]*)")?\s*$'
)

DATE_FMT = "%d/%b/%Y:%H:%M:%S %z"

BOT_IPS = {
    '70.95.19.135',      # your home IP
}

BOT_IP_PREFIXES = [
    '43.',               # Tencent Cloud bots
    '47.',                # Alibaba Cloud
    '35.199.',            # Google Cloud scanner
    '46.161.',            # Netherlands probe
    '93.174.',            # Netherlands bot
    '190.2.',             # DomainIPScanner
    '176.32.',            # Russia bot
    '172.68.',            # Cloudflare proxy scanner
    '172.69.',            # Cloudflare proxy scanner
    '104.210.',           # OpenAI SearchBot
    '168.100.',           # AhrefsBot
    '198.235.',           # Palo Alto Networks
    '136.0.',             # credential scanner
    '104.239.',           # credential scanner
    '185.242.',           # SSL probe
    '180.153.',           # 360Spider China
    '192.36.',            # Akamai CDN (ambiguous)
]

BOT_UA_KEYWORDS = [
    'zgrab', 'bot', 'Bot', 'crawler', 'Crawler', 'spider', 'Spider',
    'scanner', 'Scanner', 'curl/', 'python-requests', 'DomainIP',
    'Palo Alto', 'Amazonbot', 'AhrefsBot', 'SearchBot', 'Gort',
    'Infrawatch', 'InternetMeasurement', 'visionheight', 'Censys',
    'iPhone OS 13_2_3',   # fake iPhone UA used by Tencent bots
]

BOT_PATHS = [
    '/.env', '/wp-admin', '/wp-includes', '/xmlrpc', '/feed/',
    '/cgi-bin', '/hudson', '/ReportServer', '/ecp/', '/aaa', '/aab',
    '/robots.txt', '/favicon.ico',
]


def parse_log_line(line: str) -> dict | None:
    m = LOG_RE.match(line.strip())
    if not m:
        return None
    d = m.groupdict()
    try:
        d['dt'] = datetime.strptime(d['date'], DATE_FMT)
    except ValueError:
        return None
    try:
        d['status'] = int(d['status'])
    except ValueError:
        d['status'] = 0
    return d


def is_bot(row: dict) -> bool:
    ip = row['ip']
    ua = row['ua'] or ''
    path = row['path'] or ''

    if ip in BOT_IPS:
        return True
    if ip.startswith(tuple(BOT_IP_PREFIXES)):
        return True
    if any(kw in ua for kw in BOT_UA_KEYWORDS):
        return True
    if any(bp in path for bp in BOT_PATHS):
        return True
    return False


def score_human(ip: str, ua: str, referer: str, path: str, status: int, method: str) -> tuple[int, str]:
    """
    Returns (score 0-100, reason)
    Higher = more likely human
    """
    score = 50
    reasons = []

    # Positive signals
    if referer and 'miacorpdata.com' in referer:
        score += 20
        reasons.append('has miacorpdata referer')
    if any(p in path for p in ['/mlb-dashboard', '/briefing', '/mlb/', '/hitter/', '/pitcher/']):
        score += 10
        reasons.append('visited real page')
    if 'Safari' in ua and 'iPhone OS 17' in ua:
        score += 15
        reasons.append('modern iPhone')
    if 'Safari' in ua and 'Version/17' in ua:
        score += 15
        reasons.append('modern Safari')
    if 'Firefox' in ua and 'bot' not in ua.lower():
        score += 10
        reasons.append('Firefox')
    if status == 200:
        score += 5

    # Negative signals
    if 'Linux x86_64' in ua and 'Chrome' in ua:
        score -= 20
        reasons.append('headless Chrome pattern')
    if ip.startswith(('3.', '52.', '54.', '34.')):  # AWS IPs
        score -= 30
        reasons.append('AWS IP range')
    if ip.startswith(('146.190.', '157.173.', '185.218.')):  # DO/VPS
        score -= 25
        reasons.append('VPS/cloud IP')
    if 'Chrome/98' in ua or 'Chrome/91' in ua or 'Chrome/95' in ua:
        score -= 20
        reasons.append('outdated Chrome version')

    return max(0, min(100, score)), ', '.join(reasons)


def confidence_bucket(score: int) -> str:
    if score >= 70:
        return 'high'
    if score >= 50:
        return 'medium'
    return 'low'


def fmt_row(row: dict, score: int, reasons: str) -> str:
    dt_str = row['dt'].strftime('%b %d %H:%M:%S')
    return f"{dt_str:<20} {row['ip']:<17}{row['path']:<24}{score:<7}{reasons}"


def build_report(rows: list[dict], since: date_cls | None) -> str:
    total = len(rows)

    kept = [r for r in rows if not is_bot(r)]
    bot_count = total - len(kept)

    scored = []
    for r in kept:
        score, reasons = score_human(r['ip'], r['ua'], r['referer'], r['path'], r['status'], r['method'])
        scored.append((r, score, reasons))

    high = [t for t in scored if confidence_bucket(t[1]) == 'high']
    medium = [t for t in scored if confidence_bucket(t[1]) == 'medium']
    low = [t for t in scored if confidence_bucket(t[1]) == 'low']

    high.sort(key=lambda t: t[0]['dt'])
    medium.sort(key=lambda t: t[0]['dt'])

    if rows:
        dts = [r['dt'] for r in rows]
        period_start = min(dts)
        period_end = max(dts)
        if since:
            period_start = max(period_start, datetime.combine(since, datetime.min.time()).replace(tzinfo=period_start.tzinfo))
        period_str = f"{period_start.strftime('%b %d')} - {period_end.strftime('%b %d, %Y')}"
    else:
        period_str = "(no data)"

    lines = []
    lines.append("MIACORPDATA.COM TRAFFIC REPORT")
    lines.append(f"Period: {period_str}")
    lines.append("=" * 48)
    lines.append("")
    lines.append("SUMMARY")
    human_pct = (len(kept) / total * 100) if total else 0.0
    bot_pct = (bot_count / total * 100) if total else 0.0
    lines.append(f"Total requests:          {total:,}")
    lines.append(f"Bot/scanner requests:    {bot_count:,} ({bot_pct:.1f}%)")
    lines.append(f"Possible human requests: {len(kept):,} ({human_pct:.1f}%)")
    lines.append(f"High confidence humans:  {len(high)}")
    lines.append(f"Medium confidence:       {len(medium)}")
    lines.append(f"Low confidence:          {len(low)}")
    lines.append("")

    ROW_LIMIT = 100

    lines.append("=" * 48)
    lines.append("HIGH CONFIDENCE HUMAN VISITS (score >= 70)")
    lines.append("=" * 48)
    lines.append(f"{'DATE/TIME (UTC)':<20} {'IP':<17}{'PAGE':<24}{'SCORE':<7}SIGNALS")
    if high:
        for row, score, reasons in high[:ROW_LIMIT]:
            lines.append(fmt_row(row, score, reasons))
        if len(high) > ROW_LIMIT:
            lines.append(f"... ({len(high) - ROW_LIMIT} more not shown)")
    else:
        lines.append("(none)")
    lines.append("")

    lines.append("=" * 48)
    lines.append("MEDIUM CONFIDENCE (score 50-69)")
    lines.append("=" * 48)
    lines.append(f"{'DATE/TIME (UTC)':<20} {'IP':<17}{'PAGE':<24}{'SCORE':<7}SIGNALS")
    if medium:
        for row, score, reasons in medium[:ROW_LIMIT]:
            lines.append(fmt_row(row, score, reasons))
        if len(medium) > ROW_LIMIT:
            lines.append(f"... ({len(medium) - ROW_LIMIT} more not shown — "
                          f"see SUMMARY/UNIQUE IPS counts above for full totals)")
    else:
        lines.append("(none)")
    lines.append("")

    lines.append("=" * 48)
    lines.append("TOP PAGES VISITED (humans only)")
    lines.append("=" * 48)
    page_counts = Counter(r['path'] for r, _, _ in scored)
    for path, count in page_counts.most_common(10):
        lines.append(f"{path:<18}{count} visits")
    if not page_counts:
        lines.append("(none)")
    lines.append("")

    lines.append("=" * 48)
    lines.append("UNIQUE IPS BY CONFIDENCE")
    lines.append("=" * 48)
    high_ips = {row['ip'] for row, _, _ in high}
    medium_ips = {row['ip'] for row, _, _ in medium}
    low_ips = {row['ip'] for row, _, _ in low}
    lines.append(f"High (>=70):    {len(high_ips)} unique IPs")
    lines.append(f"Medium (50-69): {len(medium_ips)} unique IPs")
    lines.append(f"Low (<50):      {len(low_ips)} unique IPs")
    lines.append("")

    lines.append("=" * 48)
    lines.append("GEOGRAPHIC HINTS (based on IP ranges)")
    lines.append("=" * 48)
    lines.append("(note: requires manual IP lookup — see IPs above)")

    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description="Analyze nginx access logs for real human traffic.")
    parser.add_argument('--log', help='Path to nginx access log (combined format). Reads stdin if omitted.')
    parser.add_argument('--since', help='Only include requests on/after this date (YYYY-MM-DD).')
    parser.add_argument('--output', help='Write report to this path instead of stdout.')
    args = parser.parse_args()

    since = None
    if args.since:
        since = datetime.strptime(args.since, '%Y-%m-%d').date()

    if args.log:
        with open(args.log, 'r', errors='replace') as f:
            lines = f.readlines()
    else:
        lines = sys.stdin.readlines()

    rows = []
    for line in lines:
        row = parse_log_line(line)
        if row is None:
            continue
        if since and row['dt'].date() < since:
            continue
        rows.append(row)

    report = build_report(rows, since)

    if args.output:
        with open(args.output, 'w') as f:
            f.write(report)
    else:
        print(report)


if __name__ == "__main__":
    main()

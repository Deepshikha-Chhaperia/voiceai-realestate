"""Forwardable printable client report. All data is escaped, no scripts/external assets."""
from html import escape
from datetime import datetime,timezone


def report_html(report):
    def e(value):return escape('' if value is None else str(value))
    tz=__import__('zoneinfo').ZoneInfo(report.get('timezone') or 'UTC')
    start=datetime.fromtimestamp(report['since'],tz).strftime('%d %b %Y')
    end=datetime.fromtimestamp(report['until'],tz).strftime('%d %b %Y')
    rows=report.get('crm',[])
    counts={k:sum(r.get('tier')==k for r in rows) for k in ('hot','warm','cold','pending')}
    callcount=sum(int(r.get('count',0)) for r in report.get('calls',[]))
    cards=''.join(f'<section class="card"><div>{e(k.title())}</div><strong>{v}</strong></section>' for k,v in counts.items())
    table=''.join('<tr>'+''.join(f'<td>{e(v)}</td>' for v in [r.get('name') or 'Caller',r.get('phone'),r.get('tier'),r.get('score'),r.get('reason'),
      ', '.join(f"{v.get('day') or ''} {v.get('time') or ''}: {v.get('status') or ''}" for v in r.get('visits',[])) or 'No recorded visit'])+'</tr>' for r in rows)
    if not table:table='<tr><td colspan="6">No caller touchpoints in this period.</td></tr>'
    return '''<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Fortnightly lead report</title>
<style>body{font:15px/1.55 Arial,sans-serif;color:#17253a;background:#eef2f7;margin:0}.page{max-width:1080px;margin:32px auto;background:white;padding:40px;border-radius:12px}h1{font-size:30px;margin:0}h2{font-size:19px;margin:32px 0 12px}.muted{color:#607085}.cards{display:grid;grid-template-columns:repeat(4,1fr);gap:14px}.card{border:1px solid #dce3eb;padding:18px;border-radius:8px}.card strong{display:block;font-size:30px}table{width:100%;border-collapse:collapse;font-size:13px}th,td{padding:12px;text-align:left;vertical-align:top;border-bottom:1px solid #dce3eb;overflow-wrap:anywhere}th{background:#f1f5f9}th:nth-child(2),td:nth-child(2),th:nth-child(3),td:nth-child(3),th:nth-child(4),td:nth-child(4){white-space:nowrap}footer{margin-top:28px;font-size:12px;color:#607085}@media(max-width:760px){.page{padding:20px;margin:0;border-radius:0}.cards{grid-template-columns:repeat(2,1fr)}.table{overflow:auto}.table table{min-width:850px}}@media print{body{background:white}.page{padding:0;margin:0;border:none}tr{break-inside:avoid}footer{position:static}}</style><main class="page">'''+f'''<div class="muted">{e(report.get('brand'))}</div><h1>Fortnightly lead report</h1><p class="muted">{e(start)} to {e(end)} (exclusive end, {e(report.get('timezone') or 'UTC')} data window)</p>
<p>{callcount} tracked calls · {len(rows)} caller records with activity</p><div class="cards">{cards}</div><h2>Caller qualification and visits</h2><div class="table"><table><thead><tr><th>Caller</th><th>Phone</th><th>Tier</th><th>Score</th><th>Reason</th><th>Visit status</th></tr></thead><tbody>{table}</tbody></table></div>
<footer>Prepared from recorded call activity. Pending means evidence is incomplete, not a cold lead. Requested visits are not confirmed bookings. Contains private caller details: review the recipient before sharing. This report has not been sent automatically.</footer></main></html>'''

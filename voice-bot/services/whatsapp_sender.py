"""Compatibility wrapper. All automatic delivery uses readiness-gated Meta transport."""
from enterprise.config import e164
from enterprise.meta import send


def normalize_phone_e164(phone):
    value=str(phone or '').strip()
    if value.isdigit() and len(value)==12 and value.startswith('91'):value='+'+value
    if value.isdigit() and len(value)==10 and value[0] in '6789':value='+91'+value
    return value if e164(value) else ''


async def send_whatsapp_location(to_phone,*,visit_date,visit_time,project_name='',client_name='',**kwargs):
    return await send({'action':'location','phone':normalize_phone_e164(to_phone),'visit_date_iso':visit_date,
      'time_slot':visit_time,'project_name':project_name,'name':client_name,**kwargs})

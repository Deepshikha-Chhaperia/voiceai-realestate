"""Provider webhook URL-encoded parser. No multipart upload surface."""
from urllib.parse import parse_qsl
from fastapi import HTTPException


async def provider_form(request):
    kind=request.headers.get('content-type','').split(';',1)[0].lower()
    body=await request.body()
    if len(body)>128*1024:raise HTTPException(413)
    if not body:return {}
    if kind!='application/x-www-form-urlencoded':raise HTTPException(415,detail='URL encoded webhook required')
    try: pairs=parse_qsl(body.decode('utf-8'),keep_blank_values=True,max_num_fields=1000)
    except (ValueError,UnicodeError):raise HTTPException(400)
    if len({k for k,v in pairs})!=len(pairs):raise HTTPException(400,detail='Duplicate webhook fields')
    return dict(pairs)

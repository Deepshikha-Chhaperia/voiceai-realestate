"""Provider-side recording deletion with verification.

Vobiz documents recording list/retrieve/download, and its overview page says recordings can be deleted, but there
is NO documented delete endpoint page. Contract used here is therefore UNVERIFIED: DELETE
/api/v1/Account/{auth_id}/Recording/{recording_id}/ . The DELETE response is never trusted. Deletion counts only
when a follow-up GET of the same recording returns 404. Anything else stays 'provider_delete_pending' for an
operator (delete in the Vobiz console, then it is confirmed by the same GET check on the next pass).
"""
import os
import re
import httpx


async def delete_recording(recording_id, client_factory=None):
    aid, tok = os.getenv('VOBIZ_AUTH_ID', '').strip(), os.getenv('VOBIZ_AUTH_TOKEN', '').strip()
    if not aid or not tok:
        return {'state': 'not_ready', 'reason': 'Vobiz credentials missing'}
    if not re.fullmatch(r'[A-Za-z0-9_-]{6,128}', str(recording_id or '')):
        return {'state': 'not_ready', 'reason': 'recording id invalid'}
    url = f'https://api.vobiz.ai/api/v1/Account/{aid}/Recording/{recording_id}/'
    headers = {'X-Auth-ID': aid, 'X-Auth-Token': tok}
    try:
        async with (client_factory or httpx.AsyncClient)(timeout=8, follow_redirects=False) as c:
            pre = await c.get(url, headers=headers)
            if pre.status_code == 404:
                return {'state': 'deleted_verified', 'note': 'already absent (GET 404)'}
            d = await c.delete(url, headers=headers)
            post = await c.get(url, headers=headers)
        if post.status_code == 404:
            return {'state': 'deleted_verified', 'delete_status': d.status_code}
        return {'state': 'delete_unverified', 'delete_status': d.status_code, 'get_status': post.status_code,
                'note': 'undocumented endpoint; delete manually in the Vobiz console'}
    except Exception as exc:
        return {'state': 'delete_unverified', 'reason': type(exc).__name__}

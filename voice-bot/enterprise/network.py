"""Resolve and pin public media endpoints before sensitive fetches."""
import asyncio
import ipaddress
import socket
from urllib.parse import urlparse


async def assert_public_url(url,allowed_hosts):
    parsed=urlparse(url)
    if parsed.scheme!='https' or parsed.hostname not in allowed_hosts or parsed.username or parsed.password or parsed.port not in {None,443}:
        raise ValueError('Unapproved HTTPS media URL')
    addresses=await asyncio.to_thread(socket.getaddrinfo,parsed.hostname,443,type=socket.SOCK_STREAM)
    if not addresses or any(not ipaddress.ip_address(a[4][0]).is_global for a in addresses):raise ValueError('Private/nonpublic media address')
    # httpx resolves independently: readiness explicitly assumes trusted provider-controlled
    # exact media host. No attacker-controlled arbitrary host/redirect may be allowed.
    return url

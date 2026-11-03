from urllib.parse import urlparse

import tldextract


def _registered_domain(origin: str) -> str:
    """Extract registered domain from a full origin string."""
    if not origin:
        return ""
    try:
        parsed = urlparse(origin)
        host = parsed.hostname or origin
        ext = tldextract.extract(host)
        return ext.registered_domain.lower() if ext.registered_domain else host.lower()
    except Exception:
        return ""

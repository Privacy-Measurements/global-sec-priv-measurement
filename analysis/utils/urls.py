from urllib.parse import urlparse

import tldextract


# Convert a request URL into its tracker domain (eTLD+1).
def extract_domain_from_url(request_url):
    try:
        hostname = urlparse(request_url).hostname
        if not hostname:
            return None
        ext = tldextract.extract(hostname)
        if not ext.domain or not ext.suffix:
            return None
        return f"{ext.domain}.{ext.suffix}"
    except Exception:
        return None


def normalize_url(url):
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"


def is_http_url(value):
    # Accept only HTTP(S) values with a non-empty hostname.
    if value is None:
        return False
    try:
        parsed = urlparse(str(value).strip())
        return parsed.scheme in {"http", "https"} and bool(parsed.netloc)
    except Exception:
        return False


def normalize_domain(domain):
    # Normalize domains for equality checks (case-insensitive, strips leading www).
    if not domain:
        return ""
    d = str(domain).strip().lower()
    if d.startswith("www."):
        d = d[4:]
    return d

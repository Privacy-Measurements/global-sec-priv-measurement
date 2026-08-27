"""HAR file parser — extracts network requests from a .har file matching a .graphml."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse


class HARParser:
    """Parse a HAR file to extract network requests with first/third-party classification."""

    def __init__(self, har_path: str) -> None:
        self.har_path = Path(har_path)
        self.har_data = self._load_har()
        self.main_domain = self._get_main_domain()

    # ------------------------------------------------------------------
    # Loading
    # ------------------------------------------------------------------

    def _load_har(self) -> Dict:
        try:
            with open(self.har_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}

    # ------------------------------------------------------------------
    # Domain helpers
    # ------------------------------------------------------------------

    def _get_main_domain(self) -> str:
        try:
            pages = self.har_data.get("log", {}).get("pages", [])
            if pages:
                first_page_url = pages[0].get("title", "")
                if not first_page_url:
                    entries = self.har_data.get("log", {}).get("entries", [])
                    if entries:
                        first_page_url = entries[0].get("request", {}).get("url", "")
                parsed = urlparse(first_page_url)
                return parsed.netloc.split(":")[0]
        except Exception:
            pass
        return ""

    def _get_domain(self, url: str) -> str:
        try:
            return urlparse(url).netloc.split(":")[0]
        except Exception:
            return ""

    def _get_origin(self, url: str) -> str:
        try:
            parsed = urlparse(url)
            return f"{parsed.scheme}://{parsed.netloc}"
        except Exception:
            return ""

    def _is_first_party(self, url: str) -> bool:
        domain = self._get_domain(url)
        if not domain or not self.main_domain:
            return False
        main_parts = self.main_domain.split(".")
        url_parts = domain.split(".")
        main_root = ".".join(main_parts[-2:]) if len(main_parts) >= 2 else self.main_domain
        url_root = ".".join(url_parts[-2:]) if len(url_parts) >= 2 else domain
        return main_root == url_root

    # ------------------------------------------------------------------
    # Resource type inference
    # ------------------------------------------------------------------

    def _infer_resource_type(self, request_headers: Dict, mime_type: str, url: str) -> str:
        mime_lower = mime_type.lower()
        url_lower = url.lower()

        sec_dest = request_headers.get("sec-fetch-dest", "")
        if sec_dest == "document":
            return "document"
        if sec_dest in ("iframe", "frame"):
            return "subdocument"
        if any(t in mime_lower for t in ["javascript", "ecmascript", "application/x-javascript"]):
            return "script"
        if "text/css" in mime_lower:
            return "stylesheet"
        if mime_lower.startswith("image/"):
            return "image"
        if mime_lower.startswith("font/") or "font" in mime_lower:
            return "font"
        if mime_lower.startswith(("video/", "audio/")):
            return "media"
        if any(t in mime_lower for t in ["json", "xml"]):
            return "xhr"

        if any(url_lower.endswith(ext) for ext in [".js", ".mjs"]):
            return "script"
        if ".css" in url_lower:
            return "stylesheet"
        if any(ext in url_lower for ext in [".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".ico"]):
            return "image"
        if any(ext in url_lower for ext in [".woff", ".woff2", ".ttf", ".otf", ".eot"]):
            return "font"
        if any(ext in url_lower for ext in [".mp4", ".webm", ".ogg", ".mp3", ".wav"]):
            return "media"
        if (
            request_headers.get("sec-fetch-mode", "") == "navigate"
            or (
                request_headers.get("sec-fetch-dest", "") != "empty"
                and mime_type != "text/plain"
                and request_headers.get("sec-fetch-user", "?0") == "?0"
            )
        ):
            return "subdocument"
        if sec_dest:
            return sec_dest
        if "text/html" in mime_lower:
            return "document"
        return "other"

    # ------------------------------------------------------------------
    # Frame detection
    # ------------------------------------------------------------------

    def _mark_frames(self, requests: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        main_frame_urls: set = set()
        sub_frame_url_to_origin: Dict[str, str] = {}

        current_main_chain: List[Dict] = []
        current_sub_chains: Dict[str, List[Dict]] = {}

        for r in requests:
            status = r["status"]
            resource_type = r["resource_type"]

            if resource_type not in ("document", "subdocument"):
                continue

            is_redirect = 300 <= status < 400

            if resource_type == "document":
                current_main_chain.append(r)
                main_frame_urls.add(r["url"])
                if not is_redirect:
                    current_main_chain = []

            elif resource_type == "subdocument":
                initiator_url = r["initiator"].get("url", "")
                matched = False
                for chain_key, chain in list(current_sub_chains.items()):
                    if chain and is_redirect and abs(r["time"] - chain[-1]["time"]) < 100:
                        chain.append(r)
                        matched = True
                        break
                if not matched:
                    chain_id = f"{initiator_url}_{r['url']}"
                    current_sub_chains.setdefault(chain_id, []).append(r)
                    if not is_redirect:
                        final_origin = self._get_origin(r["url"])
                        for req in current_sub_chains[chain_id]:
                            sub_frame_url_to_origin[req["url"]] = final_origin
                        del current_sub_chains[chain_id]

        # flush incomplete chains
        for chain in current_sub_chains.values():
            if chain:
                final_origin = self._get_origin(chain[-1]["url"])
                for req in chain:
                    sub_frame_url_to_origin[req["url"]] = final_origin

        for r in requests:
            url = r["url"]
            if url in main_frame_urls:
                r["is_main_frame"] = True
                r["is_sub_frame"] = False
                r["frame_origin"] = None
            elif url in sub_frame_url_to_origin:
                r["is_main_frame"] = False
                r["is_sub_frame"] = True
                r["frame_origin"] = sub_frame_url_to_origin[url]
            else:
                r["is_main_frame"] = False
                r["is_sub_frame"] = False
                r["frame_origin"] = None

        return requests

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def extract_requests(self) -> List[Dict[str, Any]]:
        """Return all network requests from the HAR with metadata."""
        entries = self.har_data.get("log", {}).get("entries", [])
        requests = []

        for entry in entries:
            request = entry.get("request", {})
            response = entry.get("response", {})
            url = request.get("url", "")

            request_headers = {
                h.get("name", ""): h.get("value", "")
                for h in request.get("headers", [])
            }
            response_headers = {
                h.get("name", ""): h.get("value", "")
                for h in response.get("headers", [])
            }

            resource_type = entry.get("_resourceType", "other") or "other"
            if resource_type == "other":
                mime_type = response.get("content", {}).get("mimeType", "")
                resource_type = self._infer_resource_type(request_headers, mime_type, url)

            initiator = entry.get("_initiator") or {}

            if isinstance(initiator, dict):
                initiator_obj = {
                    "type": initiator.get("type", "unknown"),
                    "url": initiator.get("url", ""),
                    "line_number": initiator.get("lineNumber", ""),
                    "stack": initiator.get("stack", {}),
                }
            elif isinstance(initiator, str):
                initiator_obj = {
                    "type": initiator,
                    "url": "",
                    "line_number": "",
                    "stack": {},
                }
            else:
                initiator_obj = {
                    "type": "unknown",
                    "url": "",
                    "line_number": "",
                    "stack": {},
                }

            requests.append(
                {
                    "url": url,
                    "domain": self._get_domain(url),
                    "party_type": "first-party" if self._is_first_party(url) else "third-party",
                    "resource_type": resource_type,
                    "is_main_frame": False,
                    "is_sub_frame": False,
                    "frame_origin": None,
                    "method": request.get("method", ""),
                    "status": response.get("status", ""),
                    "status_text": response.get("statusText", ""),
                    "mime_type": response.get("content", {}).get("mimeType", ""),
                    "size": response.get("content", {}).get("size", 0),
                    "time": entry.get("time", 0),
                    "started_datetime": entry.get("startedDateTime", ""),
                    "request_headers": request_headers,
                    "response_headers": response_headers,
                    "initiator": initiator_obj,
                    "server_ip": entry.get("serverIPAddress", ""),
                    "connection": entry.get("connection", ""),
                    "cache": {
                        "before": entry.get("cache", {}).get("beforeRequest", {}),
                        "after": entry.get("cache", {}).get("afterRequest", {}),
                    },
                }
            )

        return self._mark_frames(requests)

    def get_summary(self) -> Dict[str, Any]:
        """Return aggregate statistics about requests in this HAR."""
        requests = self.extract_requests()
        first_party = [r for r in requests if r["party_type"] == "first-party"]
        third_party = [r for r in requests if r["party_type"] == "third-party"]
        third_party_domains = sorted({r["domain"] for r in third_party})

        resource_type_counts: Dict[str, int] = {}
        for r in requests:
            rt = r["resource_type"]
            resource_type_counts[rt] = resource_type_counts.get(rt, 0) + 1

        return {
            "main_domain": self.main_domain,
            "total_requests": len(requests),
            "first_party_count": len(first_party),
            "third_party_count": len(third_party),
            "unique_third_party_domains": len(third_party_domains),
            "third_party_domains": third_party_domains,
            "resource_type_counts": resource_type_counts,
        }


def parse_har_for_graphml(graphml_path: str) -> Optional[Dict[str, Any]]:
    """
    Given a graphml path, find the matching .har file in the same directory
    (same stem, .har extension) and return extracted requests + summary.

    Returns None if no matching HAR file exists.
    """

    har_path = Path(graphml_path).with_suffix(".har")
    if not har_path.is_file():
        return None

    parser = HARParser(str(har_path))


    return {
        "har_path": str(har_path),
        "summary": parser.get_summary(),
        "requests": parser.extract_requests(),
    }

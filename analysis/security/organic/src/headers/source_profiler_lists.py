import json
import traceback

import tldextract

LISTS_DIR = "./privacy-lists"


def _load_disconnect() -> tuple[dict[str, str], dict[str, str]]:
    resource_to_entity: dict[str, str] = {}
    resource_to_props:  dict[str, str] = {}
    try:
        data = json.load(open(f"{LISTS_DIR}/disconnect.json"))
        for entity_name, info in data.get("entities", {}).items():
            primary = info.get("properties", [None])[0]
            for resource in info.get("resources", []):
                resource_to_entity[resource.lower()] = entity_name
                resource_to_props[resource.lower()] = primary or resource
    except Exception:
        traceback.print_exc()
    return resource_to_entity, resource_to_props


def _load_tracker_set() -> set[str]:
    trackers: set[str] = set()
    try:
        with open(f"{LISTS_DIR}/easylist_domains.txt") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    trackers.add(line.lower())
    except Exception:
        traceback.print_exc()
    try:
        origins = json.load(open(f"{LISTS_DIR}/consolidated_origins.json"))
        if isinstance(origins, list):
            for o in origins:
                trackers.add(str(o).lower().lstrip("."))
    except Exception:
        traceback.print_exc()
    return trackers


def _load_fingerprinting_domains() -> set[str]:
    domains: set[str] = set()
    try:
        data = json.load(open(f"{LISTS_DIR}/umar_iqbal_fingerprinting_list.json"))
        for entries in data.values():
            for entry in entries:
                url = entry.get("script_url", "")
                ext = tldextract.extract(url)
                if ext.registered_domain:
                    domains.add(ext.registered_domain.lower())
    except Exception:
        traceback.print_exc()
    return domains

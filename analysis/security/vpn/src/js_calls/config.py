"""
JS Calls analysis config — API taxonomy as data structures.
See API_TAXONOMY.md for full documentation and prior research citations.
"""
import sys
import os
_HERE = os.path.abspath(os.path.dirname(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, "../.."))
for _p in [_HERE, _ROOT]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

from src.headers.config import (
    DB_CONFIG, DB_CONFIG_NEW,
    DATASETS, VPN_DATASET, INCLUDE_VPN,
    RESULTS_BASE, WORKERS, QUICK_PEEK,
)

JS_CALLS_RESULTS_BASE = "./results/js_calls"

# ── API taxonomy ──────────────────────────────────────────────────────────────
# Each category maps to a set of call_method prefixes/exact matches.
# Matching is prefix-based: "Navigator.userAgent" matches call_method values
# like "Navigator.userAgent", "Navigator.userAgent.get" etc.

API_CATEGORIES: dict[str, list[str]] = {

    "fingerprinting": [
        # Navigator (FP-Inspector S&P 2021)
        "Navigator.userAgent", "Navigator.platform", "Navigator.hardwareConcurrency",
        "Navigator.deviceMemory", "Navigator.maxTouchPoints", "Navigator.languages",
        "Navigator.language", "Navigator.vendor", "Navigator.doNotTrack",
        "Navigator.cookieEnabled", "Navigator.appVersion", "Navigator.appName",
        "Navigator.product", "Navigator.productSub", "Navigator.mimeTypes",
        "Navigator.plugins", "Navigator.oscpu", "Navigator.buildID",
        # Screen
        "Screen.width", "Screen.height", "Screen.availWidth", "Screen.availHeight",
        "Screen.colorDepth", "Screen.pixelDepth",
        "window.devicePixelRatio", "window.outerWidth", "window.outerHeight",
        "window.innerWidth", "window.innerHeight", "window.screenX", "window.screenY",
        # Canvas
        "HTMLCanvasElement.toDataURL", "HTMLCanvasElement.toBlob",
        "CanvasRenderingContext2D.getImageData", "CanvasRenderingContext2D.measureText",
        "CanvasRenderingContext2D.fillText", "CanvasRenderingContext2D.strokeText",
        # WebGL
        "WebGLRenderingContext.getParameter", "WebGLRenderingContext.getExtension",
        "WebGLRenderingContext.getSupportedExtensions",
        "WebGL2RenderingContext.getParameter",
        "WebGLRenderingContext.getShaderPrecisionFormat",
        "WebGLRenderingContext.readPixels",
        # Audio
        "AudioContext.createOscillator", "AudioContext.createDynamicsCompressor",
        "AudioContext.createAnalyser", "AnalyserNode.getFloatFrequencyData",
        "AnalyserNode.getByteFrequencyData", "AudioBuffer.getChannelData",
        "BaseAudioContext.createOscillator",
        # Font
        "FontFaceSet.check", "FontFaceSet.load",
        # Timing
        "Performance.now", "performance.getEntriesByType", "performance.getEntriesByName",
        # Battery / sensors
        "Navigator.getBattery", "Navigator.getGamepads",
    ],

    "cookie_storage": [
        # Cookies (Roesner NSDI 2012, Englehardt CCS 2016)
        "Document.cookie",
        # localStorage
        "Storage.getItem", "Storage.setItem", "Storage.removeItem",
        "Storage.clear", "Storage.key", "Storage.length",
        # IndexedDB
        "IDBFactory.open", "indexedDB.open", "IDBObjectStore.put", "IDBObjectStore.get",
        "indexedDB.deleteDatabase",
        # Cache (Service Worker)
        "CacheStorage.open", "Cache.put", "Cache.match",
    ],

    "network_exfiltration": [
        # 3rd-party context only — enforced at analysis time via party filter
        # (Englehardt CCS 2016, Snyder CCS 2017)
        "XMLHttpRequest.open", "XMLHttpRequest.send", "XMLHttpRequest.setRequestHeader",
        "Navigator.sendBeacon",
        "WebSocket",
        "EventSource",
        "RTCPeerConnection", "RTCDataChannel.send",
    ],

    "eval_obfuscation": [
        # Any context — context-independent risk (Snyder CCS 2017, FP-Inspector 2021)
        "eval",
        "Function",
        "document.write", "document.writeln",
        "atob", "btoa",
        "unescape", "escape",
    ],

    "dom_surveillance": [
        # 3rd-party context only — enforced at analysis time via party filter
        # (Roesner NSDI 2012, Snyder IMC 2016)
        "MutationObserver.observe", "MutationObserver.disconnect",
        "IntersectionObserver.observe", "IntersectionObserver.disconnect",
        "ResizeObserver.observe",
        "EventTarget.addEventListener",
    ],

    "geolocation_sensors": [
        # Any context; 3rd-party = tracking signal (Snyder CCS 2017)
        "Geolocation.getCurrentPosition", "Geolocation.watchPosition",
        "Geolocation.clearWatch",
        "Navigator.vibrate",
    ],

    "defensive": [
        # Positive security signals (Web Almanac 2024 — novel contribution)
        # Trusted Types
        "TrustedTypePolicyFactory.createPolicy",
        "TrustedTypePolicyFactory.getAttributeType",
        "TrustedTypePolicyFactory.getPropertyType",
        "TrustedTypePolicyFactory.isHTML", "TrustedTypePolicyFactory.isScript",
        "TrustedTypePolicyFactory.isScriptURL",
        "TrustedTypePolicy.createHTML", "TrustedTypePolicy.createScript",
        "TrustedTypePolicy.createScriptURL",
        # Sanitizer API
        "Sanitizer", "Sanitizer.sanitize", "Sanitizer.sanitizeFor",
        "Element.setHTML",
        "Document.parseHTML",
        # Permissions
        "Permissions.query",
        # WebAuthn / Credentials
        "CredentialsContainer.get", "CredentialsContainer.create",
        "CredentialsContainer.store", "CredentialsContainer.preventSilentAccess",
    ],

    # Negative security signals — tracked separately from defensive
    "defensive_unsafe": [
        "Element.setHTMLUnsafe",
        "Document.parseHTMLUnsafe",
    ],

    "payment": [
        # Any context — commerce intent (Snyder CCS 2017 cost-benefit framework)
        "PaymentRequest",
    ],

    "ai_emerging": [
        # Novel contribution — no prior measurement study
        # On-device AI (Chrome 129+)
        "window.ai", "chrome.ai",
        "AITextSession.prompt", "AITextSession.promptStreaming",
        # WebNN
        "navigator.ml", "MLContext", "MLGraphBuilder",
        # Speech (often AI-backed)
        "SpeechRecognition", "SpeechSynthesis.speak",
        "webkitSpeechRecognition",
    ],

    "workers": [
        # 3rd-party = persistent tracking signal
        "navigator.serviceWorker.register",
        "navigator.serviceWorker.getRegistration",
        "Worker",
        "SharedWorker",
        "Worklet.addModule",
    ],
}

# Categories where only 3rd-party calls are meaningful
# (1st-party calls are functional, not a tracking/risk signal)
THIRD_PARTY_ONLY_CATEGORIES = {"network_exfiltration", "dom_surveillance"}

# Categories where 3rd-party presence is a stronger signal but 1st-party is also reported
PARTY_SENSITIVE_CATEGORIES = {"cookie_storage", "geolocation_sensors", "workers"}

# AI-related caller domains — scripts whose caller_url registered domain matches
# these are classified as making AI-related calls.
# Detected via registered_domain(caller_url), not call_args inspection.
# Extended to cover: LLM APIs, AI infrastructure, AI chatbot CDNs,
# AI analytics, AI search, vector DBs, on-device AI SDKs.
AI_CALLER_DOMAINS = {
    # ── LLM API providers ────────────────────────────────────────────────────
    "openai.com",
    "anthropic.com",
    "cohere.ai",
    "mistral.ai",
    "groq.com",
    "together.ai",
    "perplexity.ai",
    "huggingface.co",
    "replicate.com",
    "fireworks.ai",
    "ai21.com",
    "aleph-alpha.com",
    "anyscale.com",
    "deepinfra.com",
    # ── AI cloud infrastructure ───────────────────────────────────────────────
    "openai.azure.com",           # Azure OpenAI
    "bedrock.amazonaws.com",      # AWS Bedrock
    "aiplatform.googleapis.com",  # GCP Vertex AI
    "vertexai.googleapis.com",
    "generativelanguage.googleapis.com",  # Google Gemini API
    "ml.azure.com",
    # ── AI chatbot / widget CDNs ──────────────────────────────────────────────
    "chatbase.co",
    "chatsimple.ai",
    "voiceflow.com",
    "botpress.cloud",
    "botpress.com",
    "landbot.io",
    "typebot.io",
    "manychat.com",
    "chatfuel.com",
    "kommunicate.io",
    "tidio.com",
    "intercom.io",                # has AI features
    "drift.com",                  # has AI features
    # ── AI analytics / personalisation ───────────────────────────────────────
    "dynamic.yield",
    "monetate.net",
    "certona.net",
    "barilliance.com",
    "richrelevance.com",
    "evergage.com",
    "constructor.io",
    # ── AI search ────────────────────────────────────────────────────────────
    "algolia.com",                # has AI/neural search features
    "algolia.net",
    "api.perplexity.ai",
    # ── AI session replay ────────────────────────────────────────────────────
    "clarity.ms",                 # Microsoft Clarity (AI heatmaps)
    # ── Vector DBs / embedding endpoints ─────────────────────────────────────
    "pinecone.io",
    "weaviate.io",
    "qdrant.io",
    "chroma.db",
    "supabase.co",                # used as vector store
    # ── AI SDK / runtime CDNs ────────────────────────────────────────────────
    "vercel.ai",
    "sdk.vercel.ai",
    "langchain.com",
    # ── On-device AI (Chrome / Edge) ─────────────────────────────────────────
    # window.ai / chrome.ai detected via call_method, not caller_url
}

# Keep old name as alias for backward compatibility with any existing code
AI_ENDPOINT_PATTERNS = list(AI_CALLER_DOMAINS)

# JSD threshold for "significant" same-script behavioural deviation
# (FP-Inspector S&P 2021 methodology — threshold to be empirically validated)
JSD_THRESHOLD = 0.3

# Minimum number of etlds a caller_hash must appear in to be included
# in same-script deviation analysis
MIN_HASH_ETLD_PRESENCE = 3

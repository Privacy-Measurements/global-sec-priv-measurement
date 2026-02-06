
_PAGE_GRAPH_TRACKED_ITEMS = {

    #************************************************** Document and window methods **************************************************

    "Document": {
        "addEventListener", "innerHTML", "cookie", "write"
#        "domain", "referrer", "featurePolicy", "requestStorageAccess", "hasStorageAccess"
    },

   "Window": {
        "addEventListener", "showOpenFilePicker", "showSaveFilePicker", "postMessage", "navigator", "showDirectoryPicker", "fetch", "setTimeout", "setInterval", "eval", "localStorage", "sessionStorage", "indexedDB", "performance", "screen", "isSecureContext", "crossOriginIsolated"
 #        "clearTimeout", "clearInterval", "trustedTypes", "crypto", "origin"", "open", "opener", "", "name", "storage", 
    },


    "PermissionStatus": {"*"},
    "Performance": {"now"},
    "XMLHttpRequestEventTarget": { "*" },
	"ServiceWorkerRegistration": {"*"},
    "WebGLRenderingContext": {"getExtension", "getParameter", "getShaderPrecisionFormat"},
    "Screen": { "*" },


    # For IndexDB
    "IDBFactory": {"*"},
    "Window.indexedDB": {"*"},

    # For EVAL
    "Window.eval": { "*" },
    "eval": { "*" },
    "Eval": { "*" },

    #************************************************** Sensitive security APIs **************************************************

    "XMLHttpRequest": {"open", "send"},
    "Navigator": {"sendBeacon", "clipboard", "clipboard.get", "clipboard.writeText", "clipboard.write", "serviceWorker", "serviceWorker.get", "serviceWorker.register", "permissions.get", "permissions"},
    "HTMLScriptElement": {"integrity", "text", "src"},
	"WebSocket": { "send" },
    "Worker": {"postMessage"},
    "ServiceWorker": {"postMessage"},
    "BroadcastChannel": {"postMessage"},
    "SharedWorker": {"*"},


    #************************************************** CANVAS IMAGE/FONT FINGERPRINTING **************************************************

    "CanvasRenderingContext2D": {"fillText", "strokeText", "fillStyle", "fillStyle.get", "fillStyle.set", "strokeStyle", "strokeStyle.get", "strokeStyle.set", "save", "restore", "font", "font.get", "font.set", "measureText"},
    "HTMLCanvasElement": {"getContext", "toDataURL", "toBlob", "addEventListener"},
    "OffscreenCanvasRenderingContext2D": {"measureText"},


    #************************************************** WEB-RTC FINGERPRINTING **************************************************

    "RTCPeerConnection": {"createDataChannel", "createOffer", "localDescription", "localDescription.get", "onicecandidate", "onicecandidate.get"},

    #************************************************** AUDIO FINGERPRINTING **************************************************

    "BaseAudioContext": {"createOscillator", "createDynamicsCompressor", "destination", "destination.get", "startRendering", "oncomplete"},
    "AudioContext": {"createOscillator", "createDynamicsCompressor", "destination", "destination.get", "startRendering", "oncomplete"},
    "OfflineAudioContext": {"createOscillator", "createDynamicsCompressor", "destination", "destination.get", "startRendering", "oncomplete"},



    #************************************************** Things I excluded because we dont use them in analysis **************************************************

    #### NOT USED
#    "Crypto": {"digest", "subtle", "getRandomValues", "algorithms"},
#    "SubtleCrypto": {"digest", "subtle", "getRandomValues", "algorithms"},
#    "Document.requestStorageAccess": { "*" },
#    "CacheStorage": {"*"},
#    "Storage": { "*" },
#    "Cache": {"*"},
#    "CookieStore": {"*"},
#    "TrustedHTML": {"*"},
#    "TrustedScript": {"*"},
#    "TrustedScriptURL": {"*"},
#    "SecurityPolicyViolationEvent": {"*"},
#    "HTMLLinkElement": {"integrity"},
#    "MessageEvent": {"*"},
#    "MessageChannel": {"*"},
#    "HTMLIFrameElement": {"allow", "sandbox", "csp", "loading"},
#    "Request": {"*"},
#    "Response": {"*"},
#    "Headers": {"*"},
#    "XMLHttpRequestUpload": { "*" },
#    "PublicKeyCredential": {"*"},
#    "AuthenticatorResponse": {"*"},
#    "AuthenticatorAttestationResponse": {"*"},
#    "AuthenticatorAssertionResponse": {"*"},
#    "PasswordCredential": {"*"},
#    "FederatedCredential": {"*"},
#    "DeviceMotionEvent": {"*"},
#    "DeviceOrientationEvent": {"*"},
#    "AbsoluteOrientationSensor": {"*"},
#    "Accelerometer": {"*"},
#    "AmbientLightSensor": {"*"},
#    "Gyroscope": {"*"},
#    "Magnetometer": {"*"},
#    "AnalyserNode": {"*"},
#    "ReportingObserver": {"*"},
#    "PerformanceObserver": { "*" },
#    "PerformanceNavigation": { "*" },
#    "PerformanceTiming": { "*" },
#    "PerformanceNavigationTiming": {"*"},
#    "Geolocation": {"*"},
#    "Location": {"*"},
#    "MediaDevices": {"*"},
#    "WorkerGlobalScope": {"performance", "fetch"},
#    "GainNode": {"*"},
#    "OscillatorNode": {"*"},

}
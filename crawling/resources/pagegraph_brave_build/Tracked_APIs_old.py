_PAGE_GRAPH_TRACKED_ITEMS = {

    # Begin indexedDB
    "IDBFactory": {"*"},
    "Window.indexedDB": {"*"},
    # End indexedDB

    # Begin crypto 
    "Crypto": {"digest", "subtle", "getRandomValues", "algorithms"},
    "SubtleCrypto": {"digest", "subtle", "getRandomValues", "algorithms"},
    # End crypto 

    # Begin eval 
    "Window.eval": { "*" },
    "eval": { "*" },
    # End eval 
    
    # Begin Window
    "Window": {
        "performance", "fetch", "setTimeout", "setInterval", "clearTimeout", "clearInterval", "trustedTypes", "crypto", "isSecureContext", "origin", "postMessage", "crossOriginIsolated", "open", "opener", "addEventListener", "name", "localStorage", "sessionStorage", "storage", "screen", "eval", "indexedDB", "showDirectoryPicker", "showOpenFilePicker", "showSaveFilePicker"
    },
    # End Window

    # Begin Document.requestStorageAccess
    "Document.requestStorageAccess": { "*" },
    # End Document.requestStorageAccess

    # Begin storage
    "CacheStorage": {"*"},
    "Storage": { "*" },
    # End storage


    #Begin Document
    "Document": {
        "addEventListener", "domain", "referrer", "featurePolicy", "requestStorageAccess", "hasStorageAccess", "cookie", "write", "innerHTML"
    },
    #End Document 

    # Begin SharedWorker
    "SharedWorker": {"*"},
    # End sharedworker

    # Begin Worker
    "Worker": {"*"},
    # End Worker

    # Begin Cache
    "Cache": {"*"},
    # End Cache


    # Begin Permissions
    "PermissionStatus": {"*"},
    "Navigator.permissions": {"*"},
    # End permissions


    # Begin Websocket 
	"WebSocket": { "*" },
    # End Websocket

    # Begin CookieStore
    "CookieStore": {"*"},
    # End CookieStore

    "TrustedHTML": {"*"},
    "TrustedScript": {"*"},
    "TrustedScriptURL": {"*"},
    "SecurityPolicyViolationEvent": {"*"},
    "HTMLScriptElement": {"integrity", "text", "src"},
    "HTMLLinkElement": {"integrity"},
    "MessageEvent": {"*"},
    "MessageChannel": {"*"},
    "BroadcastChannel": {"*"},
    "HTMLIFrameElement": {"allow", "sandbox", "csp", "loading"},
    "Navigator": {"*"},
    "Request": {"*"},
    "Response": {"*"},
    "Headers": {"*"},
    "XMLHttpRequest": {"*"},
    "XMLHttpRequestEventTarget": { "*" },
    "XMLHttpRequestUpload": { "*" },
    "PublicKeyCredential": {"*"},
    "AuthenticatorResponse": {"*"},
    "AuthenticatorAttestationResponse": {"*"},
    "AuthenticatorAssertionResponse": {"*"},
    "PasswordCredential": {"*"},
    "FederatedCredential": {"*"},
    "DeviceMotionEvent": {"*"},
    "DeviceOrientationEvent": {"*"},
    "AbsoluteOrientationSensor": {"*"},
    "Accelerometer": {"*"},
    "AmbientLightSensor": {"*"},
    "Gyroscope": {"*"},
    "Magnetometer": {"*"},
    "CanvasRenderingContext2D": {"*"},
    "HTMLCanvasElement": {"getContext", "toDataURL", "toBlob", "addEventListener"},
    "OffscreenCanvasRenderingContext2D": {"measureText"},
    "AudioContext": {"*"},
    "BaseAudioContext": {"*"},
    "AnalyserNode": {"*"},
    "ServiceWorker": {"*"},
    "ServiceWorkerRegistration": {"*"},
    "ReportingObserver": {"*"},
    "Performance": {"*"},
    "PerformanceObserver": { "*" },
    "PerformanceNavigation": { "*" },
    "performanceNavigation": { "*" },
    "PerformanceTiming": { "*" },
    "PerformanceNavigationTiming": {"*"},
    "Geolocation": {"*"},
    "Location": {"*"},
    "MediaDevices": {"*"},
    "Screen": { "*" },
    "WorkerGlobalScope": {"performance", "fetch"},
    "WebGLRenderingContext": {"getExtension", "getParameter", "getShaderPrecisionFormat"},
	"WebGL2RenderingContext": { "getExtension", "getParameter"},

    #FROM OpenWPM
    "GainNode": {"*"},
    "OscillatorNode": {"*"},
    "OfflineAudioContext": {"*"},
    "RTCPeerConnection": {"*"},
}

from enum import Enum

class FA(Enum):
    FA_UNSAFE = 0
    CONSTRAINED = 1
    SELF = 2
    INVALID = 3

class XSS(Enum):
    XSS_UNSAFE = 0
    XSS_SAFE = 1

class TLS(Enum):
    TLS_UNSAFE = 0
    ENABLED = 1

class XFO(Enum):
    UNSAFE = 0
    SELF = 1
    NONE = 2

class HSTSAge(Enum):
    DISABLE = -1
    AGE_UNSAFE = 0
    LOW = 1
    BIG = 2

class HSTSSub(Enum):
    SUB_UNSAFE = 0
    SAFE = 1
    
class HSTSPreload(Enum):
    TOFU = 0
    TOFU_SAFE = 1

class CookieSecure(Enum):
    SECURE_UNSAFE = 0
    SECURE_SAFE = 1

class CookieHttpOnly(Enum):
    HTTP_UNSAFE = 0
    HTTP_SAFE = 1

class CookieSameSite(Enum):
    NONE = 0
    LAX = 1
    STRICT = 2
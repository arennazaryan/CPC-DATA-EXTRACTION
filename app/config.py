import os

class Config:
    SECRET_KEY = os.environ.get("SECRET_KEY") or "change_this_to_random_secret_string"

    TEMP_DATA_DIR = os.path.join(os.getcwd(), "temp_data")
    USE_S3 = False

    CPC_BASE_URL = "https://file-online.cpcarmenia.am/armepdwebservice/v1"
    MAX_WORKERS = 3
    REQUEST_TIMEOUT = 30
    USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"

    RATELIMIT_STORAGE_URI = "memory://"
    RATELIMIT_STRATEGY = "fixed-window"
    RATELIMIT_DEFAULT = "2000 per day"

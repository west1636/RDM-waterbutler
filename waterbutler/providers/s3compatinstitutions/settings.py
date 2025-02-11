try:
    from waterbutler import settings
except ImportError:
    settings = {}  # type: ignore

config = settings.get('S3COMPATINSTITUTIONS_PROVIDER_CONFIG', {})  # type: ignore

TEMP_URL_SECS = int(config.get('TEMP_URL_SECS', 100))

CONTIGUOUS_UPLOAD_SIZE_LIMIT = int(config.get('CONTIGUOUS_UPLOAD_SIZE_LIMIT', 128000000))  # 128 MB

CHUNK_SIZE = int(config.get('CHUNK_SIZE', 64000000))  # 64 MB

CHUNKED_UPLOAD_MAX_ABORT_RETRIES = int(config.get('CHUNKED_UPLOAD_MAX_ABORT_RETRIES', 2))

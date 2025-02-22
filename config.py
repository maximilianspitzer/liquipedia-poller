"""Configuration settings for the Liquipedia poller"""
import os
from typing import Dict, Callable
from exceptions import ConfigurationError
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def get_env_or_default(key: str, default: str) -> str:
    """Get environment variable with fallback to default"""
    return os.getenv(key, default)

def validate_config() -> None:
    """Validate all required configuration is present"""
    required_env_vars = [
        'DB_USER', 'DB_PASSWORD', 'DB_HOST', 'DB_PORT'  # Removed DB_NAME since we create it
    ]
    missing = [var for var in required_env_vars if not os.getenv(var)]
    if missing:
        raise ConfigurationError("Missing required environment variables", missing_keys=missing)

# Web scraping settings
REQUESTS_PER_MINUTE = int(get_env_or_default('REQUESTS_PER_MINUTE', '30'))
MAX_RETRIES = int(get_env_or_default('MAX_RETRIES', '3'))
RETRY_DELAY = int(get_env_or_default('RETRY_DELAY', '5'))
REQUEST_TIMEOUT = int(get_env_or_default('REQUEST_TIMEOUT', '30'))

# Database settings
DB_POOL_MIN_CONN = int(get_env_or_default('DB_POOL_MIN_CONN', '1'))
DB_POOL_MAX_CONN = int(get_env_or_default('DB_POOL_MAX_CONN', '10'))
DB_CONNECT_TIMEOUT = int(get_env_or_default('DB_CONNECT_TIMEOUT', '10'))

# Logging settings
LOG_FORMAT = get_env_or_default('LOG_FORMAT', '%(asctime)s - %(levelname)s - %(message)s')
LOG_FILE = get_env_or_default('LOG_FILE', 'liquipedia_poller.log')
LOG_LEVEL = get_env_or_default('LOG_LEVEL', 'INFO')

# API endpoints
BASE_URL = 'https://liquipedia.net'

# Regions to monitor
REGIONS: Dict[str, Callable[[str], bool]] = {
    'EMEA': lambda x: 'EMEA' in x,
    'North_America': lambda x: 'North_America' in x
}

# Known team names that should not be modified
NO_CLEAN_TEAMS = {
    'Tribe Gaming',
    'FUT Esports',
    'SKCalalas'
}

# Validate configuration on import
validate_config()
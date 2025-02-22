"""Custom exceptions for the Liquipedia poller"""

class LiquipediaPollerError(Exception):
    """Base exception for all Liquipedia poller errors"""
    pass

class RateLimitError(LiquipediaPollerError):
    """Raised when rate limit is exceeded"""
    def __init__(self, message="Rate limit exceeded", retry_after=None):
        self.retry_after = retry_after
        super().__init__(message)

class DatabaseError(LiquipediaPollerError):
    """Raised when database operations fail"""
    def __init__(self, message="Database operation failed", details=None):
        self.details = details
        super().__init__(message)

class ScrapingError(LiquipediaPollerError):
    """Raised when web scraping operations fail"""
    def __init__(self, message="Scraping operation failed", url=None, status_code=None):
        self.url = url
        self.status_code = status_code
        super().__init__(message)

class DataValidationError(LiquipediaPollerError):
    """Raised when data validation fails"""
    def __init__(self, message="Data validation failed", field=None, value=None):
        self.field = field
        self.value = value
        super().__init__(message)

class ConnectionError(LiquipediaPollerError):
    """Raised when connection to external services fails"""
    def __init__(self, message="Connection failed", service=None):
        self.service = service
        super().__init__(message)

class ConfigurationError(LiquipediaPollerError):
    """Raised when there are configuration issues"""
    def __init__(self, message="Configuration error", missing_keys=None):
        self.missing_keys = missing_keys or []
        super().__init__(message)
"""
Custom exceptions for the async task queue system
"""


class ValidationError(Exception):
    """Raised when validation fails"""
    pass


class SerializationError(Exception):
    """Raised when serialization/deserialization fails"""
    pass


class ConfigurationError(Exception):
    """Raised when there is a configuration error"""
    pass


class TimeoutError(Exception):
    """Raised when an operation times out"""
    pass


class RetryExhaustedError(Exception):
    """Raised when all retry attempts are exhausted"""
    pass

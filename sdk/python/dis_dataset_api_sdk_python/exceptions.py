"""Custom exception types used by the SDK."""


class ApiError(Exception):
    """Base API error raised for non-successful API responses."""

    def __init__(self, message: str, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class AuthenticationError(ApiError):
    """Raised when API authentication or authorization fails."""


class NotFoundError(ApiError):
    """Raised when a requested resource does not exist."""


class ValidationError(ApiError):
    """Raised when request validation fails."""

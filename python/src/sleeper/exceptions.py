class SleeperError(Exception):
    """Base exception for the Sleeper SDK."""


class SleeperApiError(SleeperError):
    """Returned when a Sleeper API request fails."""

    def __init__(
        self,
        status_code: int,
        message: str,
        response_body: str | None = None,
    ):
        self.status_code = status_code
        self.response_body = response_body

        super().__init__(f"HTTP {status_code}: {message}")

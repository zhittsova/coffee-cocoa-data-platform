"""Retry classification shared by the bounded source adapters."""

import urllib.error


def retryable(error):
    if isinstance(error, urllib.error.HTTPError):
        return error.code in {408, 429, 500, 502, 503, 504}
    if isinstance(error, (TimeoutError, ConnectionError, urllib.error.URLError)):
        return True
    return error.__cause__ is not None and retryable(error.__cause__)

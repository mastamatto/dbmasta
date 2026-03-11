"""Retry decorators for transient database connection failures."""
import time
import asyncio
import functools


_TRANSIENT_MESSAGES = (
    "Lost connection",
    "Can't connect",
    "Connection refused",
    "Too many connections",
    "Connection timed out",
    "server closed the connection unexpectedly",
    "connection is closed",
    "could not connect to server",
    "SSL connection has been closed unexpectedly",
    "Packet sequence number wrong",
    "Commands out of sync",
)


def is_transient(e: Exception) -> bool:
    s = str(e)
    return any(msg in s for msg in _TRANSIENT_MESSAGES)


def sync_retry(max_retries: int = 2, backoff_base: float = 0.5):
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(max_retries + 1):
                try:
                    return fn(*args, **kwargs)
                except Exception as e:
                    last_exc = e
                    if attempt < max_retries and is_transient(e):
                        time.sleep(backoff_base * (2 ** attempt))
                        continue
                    raise
            raise last_exc
        return wrapper
    return decorator


def async_retry(max_retries: int = 2, backoff_base: float = 0.5):
    def decorator(fn):
        @functools.wraps(fn)
        async def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(max_retries + 1):
                try:
                    return await fn(*args, **kwargs)
                except Exception as e:
                    last_exc = e
                    if attempt < max_retries and is_transient(e):
                        await asyncio.sleep(backoff_base * (2 ** attempt))
                        continue
                    raise
            raise last_exc
        return wrapper
    return decorator

# utils.py
import concurrent.futures
import logging
from typing import Any, Callable

LOG = logging.getLogger(__name__)


class CallTimeoutError(RuntimeError):
    pass


def call_with_timeout(func: Callable[..., Any], *args, timeout: int = 15, **kwargs) -> Any:
    """Run a synchronous function with a hard timeout using a short-lived ThreadPoolExecutor."""
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
        future = ex.submit(func, *args, **kwargs)
        try:
            return future.result(timeout=timeout)
        except concurrent.futures.TimeoutError:
            future.cancel()
            LOG.exception("call_with_timeout: function timed out")
            raise CallTimeoutError(f"Function call timed out after {timeout} seconds")


def run_in_threadpool_sync(fn: Callable, *args, **kwargs):
    """Compatibility wrapper for convenience - directly calls function."""
    return fn(*args, **kwargs)

import random
import time


def timestamp_now() -> float:
    return time.time()


def bps_to_decimal(bps: float) -> float:
    return bps / 10000.0


def add_jitter(base_seconds: float, jitter_seconds: float) -> float:
    if jitter_seconds <= 0:
        return base_seconds
    return max(0.0, base_seconds + random.uniform(-jitter_seconds, jitter_seconds))

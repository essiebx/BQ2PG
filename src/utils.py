import logging
import time
import sys
from functools import wraps


def setup_logger(name='bq2pg', level='INFO'):
    """Setup logger"""
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper()))

    # Console handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(getattr(logging, level.upper()))

    # Formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    ch.setFormatter(formatter)

    logger.addHandler(ch)
    logger.propagate = False

    return logger


def timer(func):
    """Timer decorator"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        elapsed = time.time() - start
        print(f"⏱️  {func.__name__}: {elapsed:.2f}s")
        return result
    return wrapper


logger = setup_logger()

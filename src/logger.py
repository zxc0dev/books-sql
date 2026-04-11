import logging
import os
from contextlib import contextmanager
from src.config import LOG_FILE

def _setup_root_logger():
    root = logging.getLogger()
    if root.handlers:
        return

    root.setLevel(logging.INFO)

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

    ch = logging.StreamHandler()
    ch.setFormatter(fmt)

    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setFormatter(fmt)

    root.addHandler(ch)
    root.addHandler(fh)

def get_logger(name: str) -> logging.Logger:
    _setup_root_logger()
    return logging.getLogger(name)

@contextmanager
def log_session():
    _setup_root_logger()
    try:
        yield
    finally:
        logging.shutdown()
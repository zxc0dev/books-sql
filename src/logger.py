import logging
import os
from .config import LOG_FILE
from logging.handlers import RotatingFileHandler



def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        
        fmt = logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        
        # ensure logs directory exists
        log_dir = os.path.dirname(LOG_FILE)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
        
        # console
        ch = logging.StreamHandler()
        ch.setFormatter(fmt)
        
        # file
        fh = RotatingFileHandler(LOG_FILE, maxBytes=1_000_000, backupCount=3)
        fh.setFormatter(fmt)
        
        logger.addHandler(ch)
        logger.addHandler(fh)
    
    return logger
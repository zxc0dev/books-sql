import logging
from .config import LOG_FILE

def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        
        fmt = logging.Formatter("%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
                                datefmt="%Y-%m-%d %H:%M:%S")
        
        # console
        ch = logging.StreamHandler()
        ch.setFormatter(fmt)
        
        # file
        fh = logging.FileHandler(LOG_FILE)
        fh.setFormatter(fmt)
        
        logger.addHandler(ch)
        logger.addHandler(fh)
    
    return logger
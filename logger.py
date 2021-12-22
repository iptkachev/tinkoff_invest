import logging


def get_logger():
    logging.basicConfig(level=logging.DEBUG, format='%(levelname)s - %(asctime)s - %(name)s - %(message)s')
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    return logger


def log_start_end(func):
    def wrapper(*args):
        logger = get_logger()
        logger.info(f"start {func.__name__} with args {args}")
        result = func(*args)
        logger.info(f"end {func.__name__} with args {args}")
        return result
    return wrapper

from datetime import datetime
import logging

date = datetime.now().date()
log_format = (f'%(asctime)s: [%(levelname)s] %(message)s '
              f'- (%(filename)s).%(funcName)s(%(lineno)d)')

def create_file_handler():
    file_handler = logging.FileHandler(f'_logs/{date}.log', encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter(log_format))
    return file_handler

def create_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.addHandler(create_file_handler())
    return logger

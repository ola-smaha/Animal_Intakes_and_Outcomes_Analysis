import logging

def log_error_msg(prefix_error, suffix_error):
    print(f'{prefix_error} = {suffix_error}.')

def setup_logging(log_filename='app.log'):
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logger = logging.getLogger('ETLLogger')
    logger.setLevel(logging.INFO)
    file_handler = logging.FileHandler(log_filename)
    file_handler.setFormatter(logging.Formatter(log_format))
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(log_format))
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger
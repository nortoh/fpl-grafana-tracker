import logging
import datetime

class Log(object):

    def __init__(self, name):
        self.name = name

    def setup_custom_logger(self):
        formatter = logging.Formatter(fmt='%(asctime)s - %(levelname)s - %(module)s - %(message)s')

        streamhandler = logging.StreamHandler()
        streamhandler.setFormatter(formatter)

        today = datetime.date.today()
        log_filename = f'{today}.log'

        filehandler = logging.FileHandler(f'./logs/{log_filename}')
        filehandler.setFormatter(formatter)

        logger = logging.getLogger(self.name)
        logger.setLevel(logging.INFO)
        logger.addHandler(streamhandler)
        logger.addHandler(filehandler)
        return logger
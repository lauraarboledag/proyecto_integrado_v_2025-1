import datetime
import logging
import os

class Logger:
    def __init__(self):
        if not os.path.exists('logs'):
            os.makedirs('logs')

        log_file = f"logs/bitcoin_eur_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

        logging.basicConfig(
            filename=log_file,
            level=logging.INFO,
            format='[%(asctime)s | %(levelname)s] [%(name)s.%(class_name)s.%(function_name)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        self.logger = logging.getLogger('BitcoinEUR')

    def info(self, class_name, function_name, description):
        self.logger.info(
            description,
            extra={'class_name': class_name, 'function_name': function_name}
        )

    def warning(self, class_name, function_name, description):
        self.logger.warning(
            description,
            extra={'class_name': class_name, 'function_name': function_name}
        )

    def error(self, class_name, function_name, description):
        self.logger.error(
            description,
            extra={'class_name': class_name, 'function_name': function_name}
        )
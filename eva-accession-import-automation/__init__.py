import logging
import sys


def init_logger():
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)-15s %(levelname)s %(message)s')
    result_logger = logging.getLogger(__name__)
    return result_logger


logger = init_logger()
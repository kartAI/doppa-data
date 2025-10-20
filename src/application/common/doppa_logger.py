import logging
import sys

from src import Config

Config.LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(stream=sys.stdout, level=Config.LOGGING_LEVEL)

logger = logging.getLogger(__name__)
logger.setLevel(Config.LOGGING_LEVEL)

file_handler = logging.FileHandler(Config.LOG_FILE, encoding="utf-8")
file_handler.setLevel(Config.LOGGING_LEVEL)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(Config.LOGGING_LEVEL)

formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)

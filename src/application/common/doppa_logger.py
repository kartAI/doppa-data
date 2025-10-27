import logging
import sys
import warnings

from src import Config

warnings.filterwarnings(
    "ignore",
    message="Geometry column does not contain geometry\\.",
    category=UserWarning
)

Config.LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

logging.getLogger("azure").setLevel(logging.WARNING)
logger = logging.getLogger()
logger.setLevel(Config.LOGGING_LEVEL)

if logger.hasHandlers():
    logger.handlers.clear()

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

file_handler = logging.FileHandler(Config.LOG_FILE, encoding="utf-8")
file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

logger.addHandler(console_handler)
logger.addHandler(file_handler)

import json
from datetime import date
import logging

logger = logging.getLogger(__name__)

def load_path_to_json(file_path):
    file_path = f"./data/Youtube_data_{date.today()}.json"
    try:
        logger.info(f"Loading data from JSON file: {file_path}")
        with open(file_path, "r", encoding="utf-8") as raw_data:
            data = json.load(raw_data)
        return data
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON from file: {file_path} - {e}")
        raise
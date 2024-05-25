import pandas as pd 
from pathlib import Path
import logging 

logger = logging.getLogger(__name__)

def load_data(data_file: Path) -> pd.DataFrame:
    try:
        df = pd.read_pickle(data_file)
        logger.info("DataFrame loaded successfully from: %s", data_file)
        return df
    except Exception as e:
        logger.error("Error loading DataFrame from %s: %s", data_file, e)
        return None
    
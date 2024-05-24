import pandas as pd
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

def read_data(file_path: Path):
    df = pd.read_csv(file_path)
    logger.info("Get data successfully from the %s", file_path)
    return df


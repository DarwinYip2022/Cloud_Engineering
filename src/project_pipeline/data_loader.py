""" Module to read data"""
from pathlib import Path
import logging
import pandas as pd

logger = logging.getLogger(__name__)

def read_data(file_path: Path):
    """Function to load in data
    Arguments: Path to file to be read
    Returns: pandas dataframe """
    data= pd.read_csv(file_path)
    logger.info("Get data successfully from the %s", file_path)
    return data

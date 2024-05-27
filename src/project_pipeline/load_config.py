"""Module to load config file"""
from pathlib import Path
import yaml
def load_config(config_ref: str) -> dict:
    """ Function to load config file
    Arguments: config path
    Returns: config dictionary"""
    # Load config from local path
    config_file = Path(config_ref)
    if not config_file.exists():
        raise EnvironmentError(
            f"Config file at {config_file.absolute()} does not exist"
        )

    with config_file.open(encoding="utf8") as file:
        return yaml.load(file, Loader=yaml.SafeLoader)
    
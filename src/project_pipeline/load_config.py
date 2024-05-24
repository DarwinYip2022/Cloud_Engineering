from pathlib import Path
import yaml
def load_config(config_ref: str) -> dict:
    
    # Load config from local path
    config_file = Path(config_ref)
    if not config_file.exists():
        raise EnvironmentError(
            f"Config file at {config_file.absolute()} does not exist"
        )

    with config_file.open(encoding="utf8") as f:
        return yaml.load(f, Loader=yaml.SafeLoader)
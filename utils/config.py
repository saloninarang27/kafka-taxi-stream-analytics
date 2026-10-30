"""
utils/config.py  – load config.yaml and expose as a dot-accessible dict.
"""
import yaml, pathlib

_ROOT = pathlib.Path(__file__).parent.parent
_CONFIG_PATH = _ROOT / "config.yaml"

def load_config(path=None):
    p = pathlib.Path(path) if path else _CONFIG_PATH
    with open(p, "r") as f:
        cfg = yaml.safe_load(f)
    return cfg


def kafka_cfg(cfg=None):
    cfg = cfg or load_config()
    return cfg["kafka"]

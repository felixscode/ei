from dataclasses import dataclass
from ei.utils.clogging import get_logger
from ei.utils.config_loader import load_config

try:
    from ei.utils.auto_config_types import Config
except Exception as e:
    @dataclass
    class _Config:
        value: str = "using dummy config because Config could no be loaded check if the config file is present" 
    
    Config = _Config()
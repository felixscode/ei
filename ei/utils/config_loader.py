from heracless import load_config as _load_config

CONFIG_PATH = './config/config.yaml'
DUMP_PATH = "./ei/utils/auto_config_types.py"

def load_config():
    return _load_config(CONFIG_PATH,DUMP_PATH,make_dir=True,frozen=True)

if __name__ == "__main__":
    config = load_config()
    print(config)
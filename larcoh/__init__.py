import importlib.metadata
import os

import coloredlogs
from cpg_utils import Path, to_path
from cpg_utils.config import set_config_paths

coloredlogs.install(
    level='INFO', fmt='%(asctime)s %(levelname)s (%(name)s %(lineno)s): %(message)s'
)


def get_package_name() -> str:
    """
    Get name of the package.
    """
    return __name__.split('.', 1)[0]


def get_package_path() -> Path:
    """
    Get local install path of the package.
    """
    return to_path(__file__).parent.absolute()


def get_version() -> str:
    """
    Get package version.
    """
    return importlib.metadata.version(get_package_name())


def load_config():
    larcoh_config_path = get_package_path() / 'larcoh.toml'
    assert larcoh_config_path.exists(), larcoh_config_path
    config_paths = [str(larcoh_config_path)]
    if _cpg_config_path_env_var := os.environ.get('CPG_CONFIG_PATH'):
        config_paths = config_paths + _cpg_config_path_env_var.split(',')
    set_config_paths(list(config_paths))


load_config()

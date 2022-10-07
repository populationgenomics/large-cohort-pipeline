import importlib.metadata
import logging
from cpg_utils import Path, to_path
from cpg_utils.config import get_config
from cpg_utils.hail_batch import dataset_path

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)


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


# def path_prefix(category: str | None = None) -> Path:
#     output_version = get_config()['workflow']['output_version']
#     _suffix = f'larcoh/{output_version}'
#     return to_path(dataset_path(_suffix, category=category))

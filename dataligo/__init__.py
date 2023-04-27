from .core import Ligo
from importlib import resources
try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib

# Version of the datacx package
__version__ = "0.7.1"
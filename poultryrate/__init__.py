"""
poultryrate
~~~~~~

The py_pkg package - a Python package template project that is intended
to be used as a cookie-cutter for developing new Python packages.
"""
import logging, os
from .__version__ import __version__


_levels = {
    'info': logging.INFO,
    'debug': logging.DEBUG
}

_level = os.getenv('POULTRYRATE_DEBUG', 'info')
_logLevel = _levels[_level]

if _level == "debug":
    logger = logging.getLogger()
    _output_fn = 'poultryrate.log'
    logger.setLevel(_logLevel)
    formatter = logging.Formatter('%(levelname)s:%(asctime)s:%(name)s:%(message)s')
    fileHandler = logging.FileHandler(_output_fn)
    fileHandler.setLevel(_logLevel)
    fileHandler.setFormatter(formatter)
    logger.addHandler(fileHandler)

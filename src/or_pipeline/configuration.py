"""Resolve checked-out or installed configuration without a working-directory guess."""
import os
from pathlib import Path
import sys


def config_file(name):
    if Path(name).name != name:
        raise ValueError('Configuration requires a simple file name')
    override = os.environ.get('OR_PIPELINE_CONFIG_DIR')
    candidates = ([Path(override)] if override else [
        Path(__file__).resolve().parents[2] / 'config',
        Path(sys.prefix) / 'share' / 'or_new' / 'config',
    ])
    for directory in candidates:
        target = directory / name
        if target.is_file():
            return target
    raise FileNotFoundError(f'Configuration {name} not found in checkout or installation')

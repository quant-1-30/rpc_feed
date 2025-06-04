#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import sys
import shutil
import configparser
import yaml
import inspect
import pandas as pd
import numpy as np
import warnings
from typing import Union
from yaml import SafeLoader
from tempfile import mkdtemp
from pathlib import Path
from contextlib import contextmanager
from typing import Callable, Generator
from .registry import registry
from .dt_utilty import str2date


@contextmanager
def get_temp_dir():
    dirpath = mkdtemp()
    yield dirpath
    shutil.rmtree(dirpath)


def decode_ini(ini_path="../alembic"):
    cf = configparser.ConfigParser()
    cf.read(ini_path)
    url = cf["alembic"]["sqlalchemy.url"]
    return url


def decode_yaml(yaml_path):
    with open(yaml_path, 'r') as f:
        data = yaml.load(f, Loader=SafeLoader)
        print('yaml', data)
    return data


def get_quarter_path(base_path, date_str, fmt="%Y-%m-%d"):
    """根据日期确定 dataset 路径"""
    # path = os.path.dirname(os.path.dirname(Path(__file__)))
    date = str2date(date_str, _format=fmt)
    year = date.year
    quarter = (date.month - 1) // 3 + 1
    base_path = os.path.expanduser(base_path)
    quarter_path = os.path.join(base_path, str(year), f"Q{quarter}")
    os.makedirs(quarter_path, exist_ok=True)
    return quarter_path


def build_from_cfg(obj_type):
    """Build a module from config dict.
    Args:
        cfg (dict): Config dict. It should at least contain the key "type".
        registry (:obj:`Registry`): The registry to search the type from.
        default_args (dict, optional): Default initialization arguments.
    Returns:
        obj: The constructed object.
    """
    print("registry modules", registry._module_dict.keys())
    if isinstance(obj_type, str):
        obj_cls = registry.get(obj_type)
        if obj_cls is None:
            raise KeyError(
                "{} is not in the {} registry".format(obj_type, registry)
            )
    elif inspect.isclass(obj_type):
        obj_cls = obj_type
    else:
        raise TypeError(
            "type must be a str or valid type, but got {}".format(type(obj_type))
        )
    return obj_cls()

        
def read_bin(file_path: Union[str, Path], start_index, end_index):
    file_path = Path(file_path.expanduser().resolve())
    with file_path.open("rb") as f:
        # read start_index
        ref_start_index = int(np.frombuffer(f.read(4), dtype="<f")[0])
        si = max(ref_start_index, start_index)
        if si > end_index:
            return pd.Series(dtype=np.float32)
        # calculate offset
        f.seek(4 * (si - ref_start_index) + 4)
        # read nbytes
        count = end_index - si + 1
        data = np.frombuffer(f.read(4 * count), dtype="<f")
        series = pd.Series(data, index=pd.RangeIndex(si, si + len(data)))
    return series


def recursive_glob(root_path: str, suffix: str, filter: Callable[[str], bool]) -> Generator[str, None, None]:
    """Recursively find all files under `root_path` ending with `suffix` and matching `filter` condition."""
    expand_root_path = os.path.expanduser(root_path)

    if not os.path.exists(expand_root_path):
        # asyncio need to exit
        # raise ValueError(f'{expand_root_path} not found')
        warnings.warn(f'{expand_root_path} not found')
        sys.exit(0)

    if os.path.isfile(expand_root_path) and filter(expand_root_path.split(os.sep)[-1]):
        yield expand_root_path
    else:
        for root, _, files in os.walk(expand_root_path):
            # 递归每个层级files
            for file in files:
                file_path = os.path.join(root, file)
                if file.endswith(suffix) and filter(file):
                    print("recursive_glob ", file_path)
                    yield file_path

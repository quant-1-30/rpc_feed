#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pdb
import os
import shutil
import configparser
import yaml
import inspect
import pandas as pd
import numpy as np
from typing import Union
from yaml import SafeLoader
import re
from tempfile import mkdtemp
from pathlib import Path
from contextlib import contextmanager
from .registry import registry


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


# def current():
#     path = os.path.dirname(os.path.dirname(Path(__file__)))
#     return path


# def build_from_cfg(cfg):
#     """Build a module from config dict.
#     Args:
#         cfg (dict): Config dict. It should at least contain the key "type".
#         registry (:obj:`Registry`): The registry to search the type from.
#         default_args (dict, optional): Default initialization arguments.
#     Returns:
#         obj: The constructed object.
#     """
#     print("registry modules", registry._module_dict.keys())
#     import pdb
#     pdb.set_trace()
#     assert isinstance(cfg, dict) and "type" in cfg
#     args = cfg.copy()
#     obj_type = args.pop("type")
#     obj_alias = args.pop("alias", "")
#     # pdb.set_trace()
#     if isinstance(obj_type, str):
#         obj_cls = registry.get(obj_type, alias=obj_alias)
#         if obj_cls is None:
#             raise KeyError(
#                 "{} is not in the {} registry".format(obj_alias, registry)
#             )
#     elif inspect.isclass(obj_type):
#         obj_cls = obj_type
#     else:
#         raise TypeError(
#             "type must be a str or valid type, but got {}".format(type(obj_type))
#         )
#     return obj_cls(**args)


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
    import pdb
    # assert isinstance(cfg, dict) and "type" in cfg
    # args = cfg.copy()
    # obj_type = args.pop("type")
    # obj_alias = args.pop("alias", "")
    # pdb.set_trace()
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


def recursive_glob(root_path, prefix):
    expand_root_path = os.path.expanduser(root_path)
    if os.path.isfile(expand_root_path):
        return root_path
    else:
        prefix_files = []
        for root, dirs, files in os.walk(expand_root_path):
            prefixs = [os.path.join(root, file) for file in files if file.endswith(prefix)]
            prefix_files.extend(prefixs)
            if dirs:
                for dir_ in dirs:
                    output = recursive_glob(os.path.join(root, dir_), prefix)
                    prefix_files.extend(output)
        return prefix_files


#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import re
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
from tempfile import mkdtemp, NamedTemporaryFile
from pathlib import Path
from contextlib import contextmanager
from typing import Callable, Generator
from errno import EEXIST
from os.path import exists, expanduser, join
from shutil import move, rmtree
from distutils import dir_util

from .wrapper import registry


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

def expand_path(path):
    p = os.path.expanduser(path)
    os.makedirs(p, exist_ok=True)
    return str(p)

# def get_subdirs(dataset_root, depth=1):
#     """快速扫描数据集子目录，使用 pathlib 提高性能 比os.walk 效果高"""
#     root_path = Path(dataset_root)
#     if not root_path.exists():
#         print(f"❌ Dataset root path does not exist: {dataset_root}")
#         return []

#     subdirs_1 = [
#         subdir.name for subdir in root_path.iterdir() 
#         if subdir.is_dir() and not subdir.name.startswith('.')
#     ]
#     if depth == 1:
#         return subdirs_1
#     else:
#         subdirs_2 = []
#         for subdir in subdirs_1:
#             subdirs_2.extend(get_subdirs(os.path.join(dataset_root, subdir), depth=depth-1))
#         return subdirs_2
    
def get_subdirs(dataset_root, depth=1):
    root_path = Path(dataset_root)
    if not root_path.exists():
        print(f"❌ Dataset root path does not exist: {dataset_root}")
        return []

    subdirs = [
        subdir.name for subdir in root_path.iterdir() 
        if subdir.is_dir() and not subdir.name.startswith('.')
    ]
    return subdirs


def build_from_cfg(obj_type, params):
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
    return obj_cls(**params)
        
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

def recursive_glob(root_path: str, suffix: str, pattern: str) -> Generator[str, None, None]:
    """Recursively find all files under `root_path` ending with `suffix` and matching `filter` condition."""
    expand_root_path = os.path.expanduser(root_path)
    if not os.path.exists(expand_root_path):
        # sys.exit(0)
        raise FileNotFoundError(f'{expand_root_path} not found')

    if os.path.isfile(expand_root_path) and filter(expand_root_path.split(os.sep)[-1]):
        yield expand_root_path
    else:
        print("expand_root_path ", expand_root_path)
        for root, _, files in os.walk(expand_root_path):
            for file in files:
                file_path = os.path.join(root, file)
                if file.endswith(suffix) and re.match(pattern, file):
                    print("recursive_glob ", file_path)
                    yield file_path


def hidden(path):
    """Check if a path is hidden.

    Parameters
    ----------
    path : str
        A filepath.
    """
    return os.path.split(path)[1].startswith('.')


def ensure_directory(path):
    """
    Ensure that a directory named "path" exists.
    """
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == EEXIST and os.path.isdir(path):
            return
        raise


def ensure_directory_containing(path):
    """
    Ensure that the directory containing `path` exists.

    This is just a convenience wrapper for doing::

        ensure_directory(os.path.dirname(path))
    """
    ensure_directory(os.path.dirname(path))


def ensure_file(path):
    """
    Ensure that a file exists. This will create any parent directories needed
    and create an empty file if it does not exist.

    Parameters
    ----------
    path : str
        The file path to ensure exists.
    """
    ensure_directory_containing(path)
    open(path, 'a+').close()  # touch the file


def update_modified_time(path, times=None):
    """
    Updates the modified time of an existing file. This will create any
    parent directories needed and create an empty file if it does not exist.

    Parameters
    ----------
    path : str
        The file path to update.
    times : tuple
        A tuple of size two; access time and modified time
    """
    ensure_directory_containing(path)
    os.utime(path, times)


def last_modified_time(path):
    """
    Get the last modified time of path as a Timestamp.
    """
    return pd.Timestamp(os.path.getmtime(path), unit='s', tz='UTC')


def modified_since(path, dt):
    """
    Check whether `path` was modified since `dt`.

    Returns False if path doesn't exist.

    Parameters
    ----------
    path : str
        Path to the file to be checked.
    dt : pd.Timestamp
        The date against which to compare last_modified_time(path).

    Returns
    -------
    was_modified : bool
        Will be ``False`` if path doesn't exists, or if its last modified date
        is earlier than or equal to `dt`
    """
    return exists(path) and last_modified_time(path) > dt


def zipline_root(environ=None):
    """
    Get the root directory for all zipline-managed files.

    For testing purposes, this accepts a dictionary to interpret as the os
    environment.

    Parameters
    ----------
    environ : dict, optional
        A dict to interpret as the os environment.

    Returns
    -------
    root : string
        Path to the zipline root dir.
    """
    if environ is None:
        environ = os.environ

    root = environ.get('ZIPLINE_ROOT', None)
    if root is None:
        root = expanduser('~/.zipline')

    return root


def zipline_path(paths, environ=None):
    """
    Get a path relative to the zipline root.

    Parameters
    ----------
    paths : list[str]
        List of requested path pieces.
    environ : dict, optional
        An environment dict to forward to zipline_root.

    Returns
    -------
    newpath : str
        The requested path joined with the zipline root.
    """
    return join(zipline_root(environ=environ), *paths)


def default_extension(environ=None):
    """
    Get the path to the default zipline extension file.

    Parameters
    ----------
    environ : dict, optional
        An environment dict to forwart to zipline_root.

    Returns
    -------
    default_extension_path : str
        The file path to the default zipline extension file.
    """
    return zipline_path(['extension.py'], environ=environ)


def data_root(environ=None):
    """
    The root directory for zipline data files.

    Parameters
    ----------
    environ : dict, optional
        An environment dict to forward to zipline_root.

    Returns
    -------
    data_root : str
       The zipline data root.
    """
    return zipline_path(['data'], environ=environ)


def ensure_data_root(environ=None):
    """
    Ensure that the data root exists.
    """
    ensure_directory(data_root(environ=environ))


def data_path(paths, environ=None):
    """
    Get a path relative to the zipline data directory.

    Parameters
    ----------
    paths : iterable[str]
        List of requested path pieces.
    environ : dict, optional
        An environment dict to forward to zipline_root.

    Returns
    -------
    newpath : str
        The requested path joined with the zipline data root.
    """
    return zipline_path(['data'] + list(paths), environ=environ)


def cache_root(environ=None):
    """
    The root directory for zipline cache files.

    Parameters
    ----------
    environ : dict, optional
        An environment dict to forward to zipline_root.

    Returns
    -------
    cache_root : str
       The zipline cache root.
    """
    return zipline_path(['cache'], environ=environ)


def ensure_cache_root(environ=None):
    """
    Ensure that the data root exists.
    """
    ensure_directory(cache_root(environ=environ))


def cache_path(paths, environ=None):
    """
    Get a path relative to the zipline cache directory.

    Parameters
    ----------
    paths : iterable[str]
        List of requested path pieces.
    environ : dict, optional
        An environment dict to forward to zipline_root.

    Returns
    -------
    newpath : str
        The requested path joined with the zipline cache root.
    """
    return zipline_path(['cache'] + list(paths), environ=environ)


class working_file(object):
    """A context manager for managing a temporary file that will be moved
    to a non-temporary location if no exceptions are raised in the context.

    Parameters
    ----------
    final_path : str
        The location to move the file when committing.
    *args, **kwargs
        Forwarded to NamedTemporaryFile.

    Notes
    -----
    The file is moved on __exit__ if there are no exceptions.
    ``working_file`` uses :func:`shutil.move` to move the actual files,
    meaning it has as strong of guarantees as :func:`shutil.move`.
    """
    def __init__(self, final_path, *args, **kwargs):
        self._tmpfile = NamedTemporaryFile(delete=False, *args, **kwargs)
        self._final_path = final_path

    @property
    def path(self):
        """Alias for ``name`` to be consistent with
        :class:`~zipline.util.cache.working_dir`.
        """
        return self._tmpfile.name

    def _commit(self):
        """Sync the temporary file to the final path.
        """
        move(self.path, self._final_path)

    def __enter__(self):
        self._tmpfile.__enter__()
        return self

    def __exit__(self, *exc_info):
        self._tmpfile.__exit__(*exc_info)
        if exc_info[0] is None:
            self._commit()


class working_dir(object):
    """A context manager for managing a temporary directory that will be moved
    to a non-temporary location if no exceptions are raised in the context.

    Parameters
    ----------
    final_path : str
        The location to move the file when committing.
    *args, **kwargs
        Forwarded to tmp_dir.

    Notes
    -----
    The file is moved on __exit__ if there are no exceptions.
    ``working_dir`` uses :func:`dir_util.copy_tree` to move the actual files,
    meaning it has as strong of guarantees as :func:`dir_util.copy_tree`.
    """
    def __init__(self, final_path, *args, **kwargs):
        self.path = mkdtemp()
        self._final_path = final_path

    def ensure_dir(self, *path_parts):
        """Ensures a subdirectory of the working directory.

        Parameters
        ----------
        path_parts : iterable[str]
            The parts of the path after the working directory.
        """
        path = self.getpath(*path_parts)
        ensure_directory(path)
        return path

    def getpath(self, *path_parts):
        """Get a path relative to the working directory.

        Parameters
        ----------
        path_parts : iterable[str]
            The parts of the path after the working directory.
        """
        return os.path.join(self.path, *path_parts)

    def _commit(self):
        """Sync the temporary directory to the final path.
        """
        dir_util.copy_tree(self.path, self._final_path)

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        if exc_info[0] is None:
            self._commit()
        rmtree(self.path)

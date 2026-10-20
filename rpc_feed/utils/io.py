#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import re
import sys
import fcntl
import shutil
import configparser
import yaml
import inspect
import duckdb
import warnings
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from typing import Union, Callable, Generator, Optional
from yaml import SafeLoader
from tempfile import mkdtemp, mkstemp, NamedTemporaryFile
from pathlib import Path
from contextlib import contextmanager
from errno import EEXIST
from shutil import move, rmtree
from os.path import exists, expanduser, join

from .wrapper import registry
from bt_protocol.template.duckdb_template import DAILY_AGG_SELECT



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
    file_path = Path(file_path).expanduser().resolve()
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
        raise FileNotFoundError(f'{expand_root_path} not found')

    regex = re.compile(pattern)
    filename = expand_root_path.split(os.sep)[-1]
    if os.path.isfile(expand_root_path) and regex.match(filename):
        yield expand_root_path
    else:
        for root, _, files in os.walk(expand_root_path):
            for file in files:
                if file.endswith(suffix) and regex.match(file):
                    yield os.path.join(root, file)


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
        # distutils was removed in Python 3.12; shutil.copytree(dirs_exist_ok) replaces dir_util.copy_tree
        shutil.copytree(self.path, self._final_path, dirs_exist_ok=True)

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        if exc_info[0] is None:
            self._commit()
        rmtree(self.path)


# =====================================================================
# Daily Pre-Aggregated Parquet Sync Utilities
# =====================================================================

_ASSET_TYPE_NAMES = ("benchmark", "stock", "fund")

SID_REGEX_BYTES = {
    "benchmark": re.compile(rb"^(1A0001|1B0688|2A01|399006)$"),
    "stock": re.compile(rb"^(6|0|3)\d{5}$"),
    "fund": re.compile(rb"^(51|15|16)\d{4}$"),
}

DAILY_ARROW_SCHEMA = pa.schema([
    pa.field("sid", pa.string()),
    pa.field("day", pa.int32()),
    pa.field("open", pa.int64()),
    pa.field("high", pa.int64()),
    pa.field("low", pa.int64()),
    pa.field("close", pa.int64()),
    pa.field("volume", pa.decimal128(38, 0)),
    pa.field("amount", pa.decimal128(38, 0)),
])


def resolve_daily_root(minute_root: Path) -> Path:
    minute_root = Path(minute_root).expanduser().resolve()
    if minute_root.name in _ASSET_TYPE_NAMES:
        return minute_root.parent / "daily" / minute_root.name
    return minute_root / "daily"


def daily_partition_file(daily_type_root: Path, year: str, quarter: str, date_ym: str) -> Path:
    return (
        Path(daily_type_root)
        / f"year={year}"
        / f"quarter={quarter}"
        / f"date={date_ym}"
        / "part-0.parquet"
    )


@contextmanager
def _daily_file_lock(target_file: Path):
    target_file.parent.mkdir(parents=True, exist_ok=True)
    lock_path = target_file.parent / ".daily.lock"
    with open(lock_path, "w") as lf:
        fcntl.flock(lf.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lf.fileno(), fcntl.LOCK_UN)


def _atomic_write_daily(table: pa.Table, target_file: Path, compression: str = "snappy") -> None:
    target_file.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = mkstemp(
        dir=str(target_file.parent), prefix="daily_tmp_", suffix=".parquet"
    )
    os.close(fd)
    tmp_path = Path(tmp_name)
    try:
        pq.write_table(table.cast(DAILY_ARROW_SCHEMA), tmp_path, compression=compression)
        tmp_path.replace(target_file)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise


def _merge_and_write_daily(
    con: duckdb.DuckDBPyConnection,
    new_table: pa.Table,
    out_file: Path,
) -> None:
    with _daily_file_lock(out_file):
        if out_file.exists():
            con.register("_merge_new_daily", new_table)
            try:
                escaped = str(out_file).replace("'", "''")
                merged = con.execute(
                    f"""
                    SELECT sid, day, open, high, low, close, volume, amount
                    FROM (
                        SELECT *, 1 AS __pri FROM _merge_new_daily
                        UNION ALL BY NAME
                        SELECT *, 0 AS __pri FROM read_parquet('{escaped}', hive_partitioning=false)
                    )
                    QUALIFY row_number() OVER (PARTITION BY sid, day ORDER BY __pri DESC) = 1
                    ORDER BY sid, day ASC
                    """
                ).to_arrow_table()
            finally:
                con.unregister("_merge_new_daily")
        else:
            merged = new_table
        _atomic_write_daily(merged, out_file)


@contextmanager
def _duck_session(con: Optional[duckdb.DuckDBPyConnection] = None):
    if con is not None:
        yield con
        return
    own = duckdb.connect(":memory:")
    own.execute("SET TimeZone='UTC';")
    own.execute("SET preserve_insertion_order=false;")
    try:
        yield own
    finally:
        own.close()


def upsert_daily_from_arrow_table(
    minute_type_root: Path,
    minute_arrow_table: pa.Table,
    con: Optional[duckdb.DuckDBPyConnection] = None,
) -> int:
    if minute_arrow_table is None or minute_arrow_table.num_rows == 0:
        return 0

    daily_root = resolve_daily_root(Path(minute_type_root))
    updated = 0
    with _duck_session(con) as db:
        db.register("min_batch", minute_arrow_table)
        try:
            agg_tbl = db.execute(
                f"""
                SELECT
                    CAST(year AS VARCHAR) AS y,
                    CAST(quarter AS VARCHAR) AS q,
                    CAST(date AS VARCHAR) AS ym,
                    {DAILY_AGG_SELECT}
                FROM min_batch
                WHERE close IS NOT NULL AND close > 0
                GROUP BY 1, 2, 3, 4, 5
                ORDER BY y, q, ym, sid, day ASC
                """
            ).to_arrow_table()
        finally:
            db.unregister("min_batch")

        if agg_tbl.num_rows == 0:
            return 0

        db.register("_agg_daily", agg_tbl)
        try:
            parts = db.execute(
                "SELECT DISTINCT y, q, ym FROM _agg_daily ORDER BY y, q, ym"
            ).fetchall()
            for y, q, ym in parts:
                new_daily = db.execute(
                    """
                    SELECT sid, day, open, high, low, close, volume, amount
                    FROM _agg_daily
                    WHERE y = ? AND q = ? AND ym = ?
                    ORDER BY sid, day ASC
                    """,
                    [y, q, ym],
                ).to_arrow_table()
                out_file = daily_partition_file(daily_root, y, q, ym)
                _merge_and_write_daily(db, new_daily, out_file)
                updated += 1
        finally:
            db.unregister("_agg_daily")
    return updated

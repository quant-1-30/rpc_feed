#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import signal
import asyncio
import platform
import multiprocessing
import numpy as np
import pandas as pd
import polars as pl


_STRUCT_TO_NP = {
    "b": "i1", "B": "u1",
    "h": "i2", "H": "u2",
    "i": "i4", "I": "u4", "l": "i4", "L": "u4",
    "q": "i8", "Q": "u8",
    "f": "f4", "d": "f8",
}

_POLARS_DTYPES = {
    "str": pl.String, "string": pl.String, "object": pl.String,
    "int": pl.Int64, "int64": pl.Int64, "int32": pl.Int32, "int16": pl.Int16, "int8": pl.Int8,
    "uint64": pl.UInt64, "uint32": pl.UInt32, "uint16": pl.UInt16, "uint8": pl.UInt8,
    "float": pl.Float64, "float64": pl.Float64, "float32": pl.Float32,
    "bool": pl.Boolean, "boolean": pl.Boolean,
}


def ensure_polars(meta) -> pl.DataFrame:
    """Polars DataFrame"""
    if isinstance(meta, pl.DataFrame):
        return meta
    if isinstance(meta, pd.DataFrame):
        return pl.from_pandas(meta)
    if meta is None:
        return pl.DataFrame()
    return pl.DataFrame(meta)


def to_polars_dtype(dt) -> pl.DataType:
    """Polars DataType"""
    if isinstance(dt, pl.DataType) or (isinstance(dt, type) and issubclass(dt, pl.DataType)):
        return dt
    return _POLARS_DTYPES.get(str(dt).lower().strip(), pl.String)


def build_struct_np_dtype(pack: str, lines: list, buflen: int):
    """struct zero_copy NumPy structure dtype"""
    fmt = pack.lstrip("=<>!@")
    endian = "<" if pack.startswith("<") else (">" if pack.startswith(">") else "=")
    if len(fmt) != len(lines) or any(ch not in _STRUCT_TO_NP for ch in fmt):
        return None
    dt = np.dtype(
        [(name, endian + _STRUCT_TO_NP[ch]) for name, ch in zip(lines, fmt)],
        align=False,
    )
    return dt if dt.itemsize == buflen else None


def fix_macos_mp(force: bool = True) -> bool:
    """
    macOS multiprocessing
        - OSError: [Errno 6] Device not configured
        - pandas/numpy 
        - fork differ spawn  
        
    Args:
        force: (default True)
        
    Returns:
        bool: 
        
    force :
    - force=False: RuntimeError when duplicate set
    - force=True: recommand
    """
    if platform.system() != "Darwin":
        print("ℹ️ nan macOS and skip multiprocessing bugfix")
        return True
    
    try:
        current_method = multiprocessing.get_start_method(allow_none=True)
        print(f"🔍multiprocessing : {current_method}")
        
        if current_method == 'spawn':
            return True
        
        # otherwise RuntimeError: context has already been set
        multiprocessing.set_start_method('spawn', force=force)
        return True
        
    except RuntimeError as e:
        if "context has already been set" in str(e):
            if not force:
                return False
            else:
                return False
        else:
            print(f"❌: {e}")
            return False


def signal_handler(loop):

    def _shutdown_handler(loop):
        print("Signal received, cancelling tasks...")
        for task in asyncio.all_tasks(loop):
            task.cancel()
        
    if platform.system() != "Windows":
        # Ctrl+C
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda: _shutdown_handler(loop))
    else:
        print("Windows system, no signal handler")

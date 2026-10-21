#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import gc
import logging
import polars as pl
from collections import namedtuple
from typing import List, Dict, Any, Optional, Tuple
from rpc_feed.utils.io import build_from_cfg
from rpc_feed.utils.wrapper import env_int


_WORKER_PIPELINE: List[Any] = []
_GC_INTERVAL = env_int("WORKER_GC_INTERVAL", 64)
_gc_counter = 0

NamedNode = namedtuple("Node", ["instance", "params"])


def convert_node_to_serializable(named_node) -> Tuple[str, Dict]:
    """
        Node (type, param)
    """
    node_type = getattr(named_node.instance, 'name', named_node.instance.__class__.__name__)
    return (node_type, named_node.params)


def init_worker(serialized_configs: List[Tuple[str, Dict]]):
    """
    Loky Worker
    """
    global _WORKER_PIPELINE
    _WORKER_PIPELINE = []
    for node_type, params in serialized_configs:
        try:
            inst = build_from_cfg(node_type, params)
            _WORKER_PIPELINE.append(inst)
        except Exception as e:
            logging.exception("[Worker] failed to initialize node %s: %s", node_type, e)
            raise

    logging.info("[Worker] PID %d initialized with %d nodes", os.getpid(), len(_WORKER_PIPELINE))


def run_pipeline_nodes(
    item: Any,
    pipeline: List[Any],
    failed_out: Optional[List[Any]] = None,
) -> Optional[Any]:
    if not pipeline:
        return item

    items = item if isinstance(item, (list, tuple)) else (item,)
    loader = pipeline[0]
    frames = []
    loaded_items = []
    for sub in items:
        try:
            df = loader.next(sub)
            if df is not None and not df.is_empty():
                frames.append(df)
                loaded_items.append(sub)
        except Exception as e:
            logging.exception("[Worker] loader failed on %s: %s", sub, e)
            if failed_out is not None:
                failed_out.append(sub)
    if not frames:
        return None

    try:
        current_data = pl.concat(frames, how="vertical_relaxed") if len(frames) > 1 else frames[0]
        for node in pipeline[1:]:
            current_data = node.next(current_data)
            if current_data is None:
                return None
        return current_data
    except Exception as e:
        logging.exception("[Worker] transform failed on batch (%d items): %s", len(loaded_items), e)
        if failed_out is not None:
            failed_out.extend(loaded_items)
        return None


def run_sync_pipeline_global(item: Any) -> Tuple[Any, List[Any]]:
    global _WORKER_PIPELINE, _gc_counter
    failed: List[Any] = []
    try:
        res = run_pipeline_nodes(item, _WORKER_PIPELINE, failed_out=failed)
        return res, failed
    except Exception as e:
        logging.exception("[Worker] run_sync_pipeline_global failed on %r: %s", item, e)
        items = item if isinstance(item, (list, tuple)) else [item]
        return None, list(items)
    finally:
        _gc_counter += 1
        if _gc_counter >= _GC_INTERVAL:
            _gc_counter = 0
            gc.collect()

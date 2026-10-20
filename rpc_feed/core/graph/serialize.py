#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import gc
import traceback
import polars as pl
from collections import namedtuple
from typing import List, Dict, Any, Tuple
from rpc_feed.utils.io import build_from_cfg


_WORKER_PIPELINE: List[Any] = []
_GC_INTERVAL = int(os.getenv("WORKER_GC_INTERVAL", 64))
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
            print(f"❌ [Worker] 初始化节点 {node_type} 失败: {e}")
            raise

    print(f"🚀 [Worker] PID {os.getpid()} 初始化完成，加载了 {len(_WORKER_PIPELINE)} 个节点")


def run_pipeline_nodes(item: Any, pipeline: List[Any]) -> Any:
    if not pipeline:
        return item

    items = item if isinstance(item, (list, tuple)) else (item,)
    loader = pipeline[0]
    frames = []
    for sub in items:
        try:
            df = loader.next(sub)
            if df is not None and not df.is_empty():
                frames.append(df)
        except Exception as e:
            print(f"❌ [Worker] loader error on {sub}: {e}")
    if not frames:
        return None

    current_data = pl.concat(frames, how="vertical_relaxed") if len(frames) > 1 else frames[0]
    for node in pipeline[1:]:
        current_data = node.next(current_data)
        if current_data is None:
            return None
    return current_data


def run_sync_pipeline_global(item: Any) -> Any:
    global _WORKER_PIPELINE, _gc_counter
    try:
        return run_pipeline_nodes(item, _WORKER_PIPELINE)
    except Exception as e:
        print(f"❌ [Worker] run_sync_pipeline_global: {e}")
        traceback.print_exc()
        return None
    finally:
        _gc_counter += 1
        if _gc_counter >= _GC_INTERVAL:
            _gc_counter = 0
            gc.collect()

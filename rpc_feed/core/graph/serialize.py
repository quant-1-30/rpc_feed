#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import gc
import time
import psutil
import traceback
import threading
import numpy as np
from collections import namedtuple
from typing import List, Dict, Any, Tuple
from rpc_feed.utils.io import build_from_cfg


_WORKER_PIPELINE: List[Any] = []

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

    print(f"🚀 [Worker] PID {os.getpid()} 初始化完成，加载了 {len(_WORKER_PIPELINE)} 个节点")

def run_sync_pipeline_global(item: Any) -> Any:
    global _WORKER_PIPELINE
    if not _WORKER_PIPELINE:
        return item
    try:
        current_data = item
        for node in _WORKER_PIPELINE:
            current_data = node.next(current_data)
            if current_data is None: # break in none in chain 
                return None
        return current_data
    except Exception as e:
        print(f"❌ [Worker] run_sync_pipeline_global: {e}")
        traceback.print_exc()
        return None
    finally:
        gc.collect() # locky reuse process

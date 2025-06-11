# graph_runner.py
import importlib
from collections import namedtuple

# 主进程中只将可序列化的结构（如：模块路径、参数）传给子进程；
# 子进程中使用这些信息重建 node.instance；
# 不再在子进程中共享复杂对象或闭包
SerializableNode = namedtuple("SerializableNode", ["module_path", "class_name", "params"])

_serialized_graph = []


def convert_node_to_serializable(named_node):
    return SerializableNode(
        module_path=named_node.instance.__module__,
        class_name=named_node.instance.__class__.__name__,
        params=named_node.params
    )

def build_node_from_serializable(serialized_node):
    module = importlib.import_module(serialized_node.module_path)
    cls = getattr(module, serialized_node.class_name)
    inst = cls(**serialized_node.params)
    return inst

def init_worker(serialized_graph_):
    global _serialized_graph
    _serialized_graph = serialized_graph_

def run_sync_pipeline_global(item):
    global _serialized_graph
    for serialized_node in _serialized_graph[:-1]:
        inst = build_node_from_serializable(serialized_node)
        item = inst.next(item)
    return item

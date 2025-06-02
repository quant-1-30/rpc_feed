# graph_runner.py
import importlib
from collections import namedtuple

SerializableNode = namedtuple("SerializableNode", ["module_path", "class_name", "params"])

_serialized_graph = []

# 主进程中只将可序列化的结构（如：模块路径、参数）传给子进程；
# 子进程中使用这些信息重建 node.instance；
# 不再在子进程中共享复杂对象或闭包

def convert_node_to_serializable(node):
    return SerializableNode(
        module_path=node.instance.__module__,
        class_name=node.instance.__class__.__name__,
        params=node.params
    )


def build_node_from_serializable(s_node):
    module = importlib.import_module(s_node.module_path)
    cls = getattr(module, s_node.class_name)
    instance = cls()
    return instance, s_node.params


def init_worker(serialized_graph_):
    global _serialized_graph
    _serialized_graph = serialized_graph_


def run_sync_pipeline_global(item):
    global _serialized_graph
    for s_node in _serialized_graph[:-1]:
        instance, params = build_node_from_serializable(s_node)
        item = instance.next(item, params)
    return item

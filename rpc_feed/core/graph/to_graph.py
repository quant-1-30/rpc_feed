# !/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import asyncio
import os
import networkx as nx
import matplotlib.pyplot as plt
import multiprocessing
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor

from .node import *
from rpc_feed.utils.loader import get_module_by_module_path
from rpc_feed.utils.io import build_from_cfg
from rpc_feed.utils.wrapper import singleton
from .util import init_worker, convert_node_to_serializable, run_sync_pipeline_global

Node = namedtuple("Node", ["instance", "params"])


@singleton
class Graph(object):
    """
        Pipeline is a class that can be used to build a pipeline of transformations.
    """
    cfg_cache = {}

    def __init__(self) -> None:
        self.graph = []
        # 重新初始化这些属性
        self.queue = asyncio.Queue()

    # def __getstate__(self):
    #     """定义序列化时的状态"""
    #     state = self.__dict__.copy()
    #     # 不序列化这些属性
    #     state['queue'] = None
    #     state['thread_pool'] = None
    #     return state

    # def __setstate__(self, state):
    #     """定义反序列化时的状态恢复"""
    #     self.__dict__.update(state)
    #     # 重新初始化这些属性
    #     self.queue = asyncio.Queue()
    #     self.thread_pool = ThreadPoolExecutor(max_workers=os.cpu_count())
   
    @classmethod
    def get_cfg(cls, cfg_path):
        basename = os.path.basename(cfg_path)
        if cfg_path not in cls.cfg_cache:
            cls.cfg_cache[basename] = get_module_by_module_path(cfg_path)
        return cls.cfg_cache[basename]
    
    def add_node(self, node: Node):
        self.graph.append(node)

    def remove(self, nodeName: str):
        self.graph = list(filter(lambda x: x.instance.name != nodeName, self.graph))

    def _build_graph(self, graph_xml):
        # 从 GraphML 文件加载图
        G = nx.read_graphml(graph_xml)
        # 检查是否为 DAG
        if nx.is_directed_acyclic_graph(G):
            # Topological sort to determine execution order
            topological_order = list(nx.topological_sort(G))
            print("Topological Sort (NetworkX):", topological_order)
            # setup pipeline
            for node in topological_order:
                node_obj = build_from_cfg(node)
                node_params = json.loads(G.nodes[node].get("params", "{}"))
                self.add_node(Node(node_obj, node_params))
        else:
            raise ValueError("The graph is not a DAG")
        print("fininsh build graph")

    async def async_consume(self):
        print("enter into async_consume")
        instance = self.graph[-1].instance
        params = self.graph[-1].params

        while True:
            item = await self.queue.get()
            if item is None:
                break
            if instance.p.is_async:
                await instance.next(item, params)
            else:
                #with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
                await asyncio.get_event_loop().run_in_executor(None, instance.next, item, params)
    
    def run_sync_pipeline(self, item):
        for node in self.graph[:-1]:
            instance = node.instance
            item = instance.next(item, node.params)
        return item

    async def _run_with_async_tail(self, iterables):
        consumer_task = asyncio.create_task(self.async_consume())

        # for iter_item in iterables:
        #     print("iter_item", iter_item)
        #     processed_item = self.run_sync_pipeline(iter_item)
        #     # async put asyncio writer into queue
        #     await self.queue.put(processed_item)

        # 避免闭包\lambda\局部函数不可 pickled 对象,主进程中只将可序列化的结构（如：模块路径、参数）传给子进程；
        # 子进程中使用这些信息重建 node.instance；
        # 不再在子进程中共享复杂对象或闭包
        serialized_graph = list(map(convert_node_to_serializable, self.graph))
        with multiprocessing.Pool(
            processes=os.cpu_count(),
            initializer=init_worker,
            initargs=(serialized_graph,)
        ) as pool:
            for processed_item in pool.imap_unordered(run_sync_pipeline_global, iterables):
                print("put into queue", len(processed_item))
                await self.queue.put(processed_item)
        # set exit signal
        await self.queue.put(None)
        await consumer_task
        
    def run(self, iterables):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self._run_with_async_tail(iterables))

    def to_execute(self, graph_xml, iterables):
        self._build_graph(graph_xml=graph_xml) 
        self.run(iterables)

    def visual_graph(self):
        # nx.circular_layout / nx.shell_layout / nx.spring_layout / nx.spectral_layout / nx.random_layout
        pos = nx.spring_layout(self.graph)
        nx.draw(self.graph, pos, with_labels=True, node_size=2000, node_color='lightblue', font_size=10, font_weight='bold')
        plt.title("Graph Visualization")
        plt.show()

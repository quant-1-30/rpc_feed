# !/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import os
import networkx as nx
import matplotlib.pyplot as plt
from concurrent.futures import ThreadPoolExecutor

from .node import *
from utils.loader import get_module_by_module_path
from utils.io import build_from_cfg
from utils.wrapper import singleton


@singleton
class Graph(object):
    """
        Pipeline is a class that can be used to build a pipeline of transformations.
    """

    def __init__(self) -> None:
        self.graph = []
   
    @classmethod
    def get_cfg(cls, cfg_path):
        basename = os.path.basename(cfg_path)
        if cfg_path not in cls.cfg_cache:
            cls.cfg_cache[basename] = get_module_by_module_path(cfg_path)
        return cls.cfg_cache[basename]
    
    def add(self, node):
        self.graph.append(node)

    def remove(self, node):
        self.graph = list(filter(lambda x: x.name != node.name, self.graph))

    def _build_graph(self, graph_xml):
        
        # 从 GraphML 文件加载图
        G = nx.read_graphml(graph_xml)
        # 检查是否为 DAG
        if nx.is_directed_acyclic_graph(G):
            # 获取拓扑排序
            topological_order = list(nx.topological_sort(G))
            print("Topological Sort (NetworkX):", topological_order)
            # Topological sort to determine execution order
            topological_order = list(nx.topological_sort(G))
            # setup pipeline
            for node in topological_order:
                node_obj = build_from_cfg(node)
                self.add_node(node_obj)
        else:
            raise ValueError("The graph is not a DAG")
        
    async def async_io(self, data):
        is_async = self.graph[-1].p.is_async
        f = self.graph[-1].next
        if not is_async:
            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
                output = await loop.run_in_executor(executor, f, data)
        else:
            output = await f(data)
        return output
    
    # @parallel(workers=6)
    def run(self, iterables):
        for item in iterables:
            for node in self.graph[:-1]:
                item = node.next(item)
            # async 
            asyncio.run(self.async_io(item))  # 运行异步任务

    def to_execute(self, graph_xml, iterables):

        self._build_graph(graph_xml=graph_xml) 
        self.run(iterables)

    def visual_graph(self):
        # nx.circular_layout / nx.shell_layout / nx.spring_layout / nx.spectral_layout / nx.random_layout
        pos = nx.spring_layout(self.graph)
        nx.draw(self.graph, pos, with_labels=True, node_size=2000, node_color='lightblue', font_size=10, font_weight='bold')
        plt.title("Graph Visualization")
        plt.show()

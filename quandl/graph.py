# !/usr/bin/env python3
# -*- coding: utf-8 -*-
import asyncio
import os
import networkx as nx
import pandas as pd
from .node import *
# from utils.wrapper import parallel
from utils.loader import get_module_by_module_path
from utils.io import build_from_cfg
import matplotlib.pyplot as plt


class Pipeline(object):
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
    
    def add_node(self, node):
        self.graph.append(node)

    def remove_node(self, node):
        self.graph = list(filter(lambda x: x.name != node.name, self.graph))

    def visual_graph(self):
        # nx.circular_layout / nx.shell_layout / nx.spring_layout / nx.spectral_layout / nx.random_layout
        pos = nx.spring_layout(self.graph)
        nx.draw(self.graph, pos, with_labels=True, node_size=2000, node_color='lightblue', font_size=10, font_weight='bold')
        plt.title("Graph Visualization")
        plt.show()

    def _build_graph(self, graph_xml, external_args=None):
        
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
                if node == "DatabaseWriter":
                    node_obj.table = external_args
                    self.add_node(node_obj)
        else:
            raise ValueError("The graph is not a DAG")

    async def async_task(self, data):
        inserts = list(data.T.to_dict().values()) if isinstance(data, pd.DataFrame) else data
        await self.graph[-1].on_handle(inserts)

    # @parallel(workers=6)
    def run(self, iterables):
        for item in iterables:
            for node in self.graph[:-1]:
                item = node.on_handle(item)
            # async 
            asyncio.run(self.async_task(item))  # 运行异步任务

    def execute_graph(self, table, graph_xml, iterables):

        self._build_graph(graph_xml=graph_xml, external_args=table) 
        self.run(iterables)

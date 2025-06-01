# !/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import os
import networkx as nx
import matplotlib.pyplot as plt
import multiprocessing

from .node import *
from rpc_feed.utils.loader import get_module_by_module_path
from rpc_feed.utils.io import build_from_cfg
from rpc_feed.utils.wrapper import singleton


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
    
    def add_node(self, node):
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

    async def async_consume(self):
        last_node = self.graph[-1]
        while True:
            item = await self.queue.get()
            if item is None:
                break
            if last_node.p.is_async:
                await last_node.next(item)
            else:
                #with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
                #    output = await loop.run_in_executor(executor, f, data)
                await asyncio.get_event_loop().run_in_executor(None, last_node.next, item)
    
    def run_sync_pipeline(self, item):
        for node in self.graph[:-1]:
            item = node.next(item)
        return item

    async def _run_with_async_tail(self, iterables):
        consumer_task = asyncio.create_task(self.async_consume())
        self.queue = multiprocessing.Queue()
        # sync process
        with multiprocessing.Pool(processes=os.cpu_count()) as pool:
            for processed_item in pool.imap_unordered(self.run_sync_pipeline, iterables):
                # async put asyncio writer into queue
                await self.queue.put(processed_item)

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

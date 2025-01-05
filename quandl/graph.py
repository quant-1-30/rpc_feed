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
        pass

    def remove_node(self, node):
        pass

    def visual_graph(self):
        pass

    def _build_graph(self, graph_xml, external_args=None):
        
        # self.G= nx.DiGraph()
        # 从 GraphML 文件加载图
        G = nx.read_graphml(graph_xml)

        if not nx.is_directed_acyclic_graph(G):
            raise ValueError("The graph is not a DAG")
      
        # Topological sort to determine execution order
        dag = list(nx.topological_sort(G))
        # setup pipeline
        for node in dag:
            node_obj = build_from_cfg(node)
            if node == "DatabaseWriter":
                node_obj.table = external_args
            self.graph.append(node_obj)

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

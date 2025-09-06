# !/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import asyncio
import os
import networkx as nx
import matplotlib.pyplot as plt
import multiprocessing
from multiprocessing import Semaphore
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor
from pyvis.network import Network

from .node import *
from .graph_mem import GraphMemoryManager
from .mp_util import init_worker, convert_node_to_serializable, run_sync_pipeline_global
from rpc_feed.utils.loader import get_module_by_module_path
from rpc_feed.utils.io import build_from_cfg
from rpc_feed.utils.wrapper import singleton
from rpc_feed.utils.mp_fix import fix_macos_mp
from rpc_feed.utils.utility import signal_handler

# 🔧 修复 macOS multiprocessing 问题
fix_macos_mp()

NamedNode = namedtuple("Node", ["instance", "params"])


@singleton
class Graph(object):
    """
        Pipeline is a class that can be used to build a pipeline of transformations.
    """
    cfg_cache = {}

    def __init__(self) -> None:
        # Graph --- writer end of graph
        self.graph = [] 
        self.w_node = []
        self.edges = None
        # async resource
        self.queue = None
        self.loop = None
        # memory manager backpressure
        # ensure consumer workers exceed q_size to avoid oom and restricted cores 
        self.memory_mgr = GraphMemoryManager()
        self.queue = asyncio.Queue(maxsize=self.memory_mgr.q_size)
        self.procs = int(os.getenv("CONCURRENT_PROCS", int(os.cpu_count() * 0.25)))
        self.consumer_workers = int(os.getenv("CONSUMER_WORKERS", os.cpu_count() * 4))
        # self.sema = Semaphore(self.consumer_workers) # control concurrency
        self._gc_event = asyncio.Event()

        self.memory_check_interval = int(os.getenv("MEMORY_CHECK_INTERVAL", "5"))

    def _init_async_resource(self, loop):
        # ENSURE UNDER ASYNCIO.LOOP OTHERWISE DEFAULT LOOP (CAUSE DIFFERENT EVENT LOOP)
        # self.loop = asyncio.get_running_loop() # not running
        self.loop = loop
        self.queue = asyncio.Queue(maxsize=self.memory_mgr.q_size)

    @classmethod
    def get_cfg(cls, cfg_path):
        basename = os.path.basename(cfg_path)
        if cfg_path not in cls.cfg_cache:
            cls.cfg_cache[basename] = get_module_by_module_path(cfg_path)
        return cls.cfg_cache[basename]
    
    def add_node(self, node: NamedNode):
        self.graph.append(node)

    def remove(self, nodeName: str):
        self.graph = list(filter(lambda x: x.instance.name != nodeName, self.graph))

    def _build_graph(self, graph_xml):
        # 从 GraphML 文件加载图
        G = nx.read_graphml(graph_xml)
        self.edges = G.edges
        # 检查是否为 DAG
        if nx.is_directed_acyclic_graph(G):
            # Topological sort to determine execution order
            topological_order = list(nx.topological_sort(G))
            print("Topological Sort (NetworkX):", topological_order)
            # setup pipeline
            for node in topological_order:
                node_params = json.loads(G.nodes[node].get("params", "{}"))
                node_obj = build_from_cfg(node, node_params)
                if not node_obj.p.is_writer:
                    self.add_node(NamedNode(node_obj, node_params))
                else:
                    self.w_node = NamedNode(node_obj, node_params)
        else:
            raise ValueError("The graph is not a DAG")
        print("fininsh build graph")

# -----------------------------------monitor----------------------------------------
    
    async def monitor_background(self):
        while not self._gc_event.is_set():
            await self.loop.run_in_executor(None, self.memory_mgr.check_memory_usage)
            await asyncio.sleep(self.memory_check_interval)

        print("exit monitor_background")
        self.memory_mgr.cleanup_gc()
        print("cleanup gc of self.memory_mgr")

# -----------------------------------consumer----------------------------------------

    # ✅ 多协程并发消费
    async def async_consume_worker(self, instance):
        while True:
            item = await self.queue.get()
            if item is None:
                break
            # async with sema # control concurrency
            if instance.p.is_async:
                await instance.next(item)
            else:
                await self.loop.run_in_executor(None, instance.next, item) # ThreadPoolExecutor

    async def async_consume(self):
        print("enter into async_consume")
        instance = self.w_node.instance
        tasks = [
            asyncio.create_task(self.async_consume_worker(instance))
            for _ in range(self.consumer_workers)
        ]
        await asyncio.gather(*tasks)
        self._gc_event.set() # exit monitor_background

# -----------------------------------produce----------------------------------------

    async def async_produce(self, pool, iterables):
        def gen():
            for item in pool.imap_unordered(run_sync_pipeline_global, iterables):
                print(f"put {len(item)} length frame into queue")
                yield item

        for p_item in gen():
            await self.queue.put(p_item)
        
        # put exit signal
        for _ in range(self.consumer_workers): # 确保所有消费者都收到 None 信号
            await self.queue.put(None)

# -----------------------------------run pipeline----------------------------------------
    
    def run_sync_pipeline(self, item):
        for node in self.graph[:-1]:
            instance = node.instance
            item = instance.next(item)
        return item

    async def _run_with_async_tail(self, iterables, parallel):

        monitor_task = asyncio.create_task(self.monitor_background())
        consumer_task = asyncio.create_task(self.async_consume())

        if parallel:
            # 避免闭包\lambda\局部函数不可 pickled 对象,主进程中只将可序列化的结构（如：模块路径、参数）传给子进程；
            # 子进程中使用这些信息重建 node.instance / 不再在子进程中共享复杂对象或闭包
            serialized_graph = list(map(convert_node_to_serializable, self.graph)) # last node is writer avoid in mp
            with multiprocessing.get_context("spawn").Pool( # 使用 spawn 模式，避免 fork 模式下子进程无法访问主进程的 asyncio 事件循环
                processes=self.procs,
                initializer=init_worker,
                initargs=(serialized_graph,)
            ) as pool:
                await self.async_produce(pool, iterables)
                pool.close() # stop receive new task

                await asyncio.gather(consumer_task, monitor_task) # ensure consumer and monitor under pool
                # ensure to avoid BrokenPipeError maybe due to main thread finish
                pool.join()
        else:
            for _, iter_item in enumerate(iterables):
                print("iter_item", iter_item)
                processed_item = self.run_sync_pipeline(iter_item)
                await self.queue.put(processed_item)
            # put exit signal
            for _ in range(self.consumer_workers): # 确保所有消费者都收到 None 信号
                await self.queue.put(None)
            await asyncio.gather(consumer_task, monitor_task) # ensure consumer and monitor under pool
        
# ----------------------------------- entrypoint ----------------------------------------

    def run(self, iterables, parallel):
        # new version get_event_loop is deprecated ( will automated create event loop old version)
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._init_async_resource(loop) # or delay get_running_loop with await 

        signal_handler(loop)

        try:
            loop.run_until_complete(self._run_with_async_tail(iterables, parallel))
        except asyncio.CancelledError:
            print("CancelledError caught, shutting down...")
        finally:
            loop.close()

    def to_execute(self, graph_xml, iterables, parallel=True):
        self._build_graph(graph_xml=graph_xml) 
        self.run(iterables, parallel)

# ----------------------------------- visual graph ----------------------------------------

    def visual_graph_html(self, output_path="dag_pipeline.html"):
        # # nx.circular_layout / nx.shell_layout / nx.spring_layout / nx.spectral_layout / nx.random_layout
        # pos = nx.spring_layout(self.graph)
        # nx.draw(self.graph, pos, with_labels=True, node_size=2000, node_color='lightblue', font_size=10, font_weight='bold')
        # plt.title("Graph Visualization")
        # plt.show()
        net = Network(directed=True, notebook=False, height='750px', width='100%')
        net.barnes_hut()  # 使用物理布局算法
    
        for meta in self.graph:
            node_data = meta.params
            node = meta.node
            label = f"{node}\n{node_data.get('type', '')}"
            title = json.dumps(node_data.get("params", {}), indent=2)
            net.add_node(node, label=label, title=title, shape="box", color="#A7C7E7")
    
        for src, dst in self.edges:
            net.add_edge(src, dst)
    
        net.show_buttons(filter_=['physics'])  # 可交互设置布局算法
        net.show(output_path)
        print(f"[Graph] DAG 可视化已生成: {output_path}")
    
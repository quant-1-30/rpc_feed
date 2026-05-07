# !/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import asyncio
import os
import networkx as nx
from collections import namedtuple
from typing import List, Any, Iterable
from loky import get_reusable_executor  # 强大的进程池，内置 cloudpickle 支持
from pyvis.network import Network

from .node import *
from .util import fix_macos_mp, signal_handler
from .serialize import init_worker, convert_node_to_serializable, run_sync_pipeline_global
from .monitor import GraphMemoryManager
from rpc_feed.utils.loader import get_module_by_module_path
from rpc_feed.utils.io import build_from_cfg
from rpc_feed.utils.wrapper import singleton

NamedNode = namedtuple("Node", ["instance", "params"])

fix_macos_mp()

@singleton
class Graph(object):
    """
    DAG 调度引擎：
    1. CPU 密集型任务由 loky (ProcessPool) 处理。
    2. I/O 密集型/异步 Writer 任务由 asyncio 协程处理。
    """
    cfg_cache = {}

    def __init__(self) -> None:
        self.graph: List[NamedNode] = [] 
        self.w_node: NamedNode = None
        self.edges = None
        
        # concurrency settings
        self.procs = int(os.getenv("CONCURRENT_PROCS", max(1, int(os.cpu_count() * 0.5))))
        self.consumer_workers = int(os.getenv("CONSUMER_WORKERS", 4))
        
        # memory management
        self.memory_mgr = GraphMemoryManager()
        self.queue = None
        self.loop = None
        self._gc_event = asyncio.Event()
        self.memory_check_interval = int(os.getenv("MEMORY_CHECK_INTERVAL", "5"))

    def _init_async_resource(self, loop):
        self.loop = loop
        # 初始化队列，maxsize 起到背压(backpressure)作用
        self.queue = asyncio.Queue(maxsize=self.memory_mgr.q_size)

    @classmethod
    def get_cfg(cls, cfg_path):
        basename = os.path.basename(cfg_path)
        if basename not in cls.cfg_cache:
            cls.cfg_cache[basename] = get_module_by_module_path(cfg_path)
        return cls.cfg_cache[basename]
    
    def _build_graph(self, graph_xml):
        G = nx.read_graphml(graph_xml)
        self.edges = G.edges
        if not nx.is_directed_acyclic_graph(G):
            raise ValueError("Graph configuration is not a DAG")

        topological_order = list(nx.topological_sort(G))
        print(f"Build graph with topological order: {topological_order}")

        for node_id in topological_order:
            params = json.loads(G.nodes[node_id].get("params", "{}"))
            node_obj = build_from_cfg(node_id, params)
            
            node_item = NamedNode(node_obj, params)
            if not node_obj.p.is_writer:
                self.graph.append(node_item)
            else:
                self.w_node = node_item
        
        if not self.w_node:
            print("Warning: No writer node defined in the graph.")

    # --- 监控逻辑 ---
    async def monitor_background(self):
        """监控内存使用情况，必要时触发 GC"""
        while not self._gc_event.is_set():
            await self.loop.run_in_executor(None, self.memory_mgr.check_memory_usage)
            await asyncio.sleep(self.memory_check_interval)
        self.memory_mgr.cleanup_gc()

    # --- 消费者逻辑 (Async Tail/Writer) ---
    async def async_consume_worker(self):
        """Writer Node"""
        instance = self.w_node.instance
        while True:
            item = await self.queue.get()
            if item is None:
                self.queue.task_done()
                break
            
            try:
                if instance.p.is_async:
                    await instance.next(item)
                else:
                    await self.loop.run_in_executor(None, instance.next, item)
            except Exception as e:
                print(f"Error in consumer worker: {e}")
            finally:
                self.queue.task_done()

    async def async_produce_parallel(self, iterables: Iterable):
        serialized_configs = [convert_node_to_serializable(n) for n in self.graph]
        
        executor = get_reusable_executor(
            max_workers=self.procs,
            initializer=init_worker,
            initargs=(serialized_configs,),
            timeout=None
        )
        loop = asyncio.get_running_loop()
        tasks = []

        for item in iterables:
            fut = loop.run_in_executor(executor, run_sync_pipeline_global, item)
            tasks.append(fut)

        for task in asyncio.as_completed(tasks):
            try:
                processed_item = await task
                if processed_item is not None:
                    await self.queue.put(processed_item)
            except Exception as e:
                print(f"Task execution failed: {e}")

        for _ in range(self.consumer_workers):
            await self.queue.put(None)

    async def _run_engine(self, iterables, parallel):
        consumers = [asyncio.create_task(self.async_consume_worker()) 
                     for _ in range(self.consumer_workers)]
        
        monitor_task = asyncio.create_task(self.monitor_background())
        try:
            if parallel:
                await self.async_produce_parallel(iterables)
            else:
                for item in iterables:
                    processed = self.run_sync_pipeline_local(item)
                    await self.queue.put(processed)
                for _ in range(self.consumer_workers):
                    await self.queue.put(None)

            await asyncio.gather(*consumers)
        finally:
            self._gc_event.set()
            await monitor_task

    def run_sync_pipeline_local(self, item):
        for node in self.graph:
            item = node.instance.next(item)
        return item
    
    def run(self, iterables, parallel):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._init_async_resource(loop)
        signal_handler(loop)
        try:
            loop.run_until_complete(self._run_engine(iterables, parallel))
        except (KeyboardInterrupt, asyncio.CancelledError):
            print("Shutting down graph...")
        finally:
            loop.close()

    def to_execute(self, graph_xml, iterables, parallel=True):
        self._build_graph(graph_xml) 
        self.run(iterables, parallel)

    def visual_graph_html(self, output_path="dag_pipeline.html"):
        net = Network(directed=True, notebook=False, height='750px', width='100%')
        net.barnes_hut()
    
        for meta in self.graph:
            label = f"{meta.instance.name}\n{meta.params.get('type', '')}"
            net.add_node(meta.instance.name, label=label, shape="box", color="#A7C7E7")
        
        if self.w_node:
            label = f"{self.w_node.instance.name}\nWriter"
            net.add_node(self.w_node.instance.name, label=label, shape="box", color="#FF7F7F")
    
        for src, dst in self.edges:
            net.add_edge(src, dst)
    
        net.show_buttons(filter_=['physics'])
        net.show(output_path)

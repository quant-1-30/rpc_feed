#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Graph 内存管理模块

专门为 Graph 类提供内存监控、优化和清理功能
"""
import os
import gc
import time
import psutil
import threading
import numpy as np
from typing import Dict, Optional, Tuple


class GraphMemoryManager:
    def __init__(self):
        # check threshold
        self.threshold = float(os.getenv("GRAPH_MAX_MEMORY_PERCENT", "75")) / 100
        self.q_size, self.max_mem = self._init()
        
    def _init(self) -> Tuple[int, float]:
        try:
            # 获取系统内存信息
            memory = psutil.virtual_memory()
            available_mb = self.threshold * memory.available / (1024 * 1024)
            q_size = int(os.getenv("GRAPH_QSIZE", int(os.cpu_count() / 4)))
            print(f"🧠 内存优化: 分配最大内存 {available_mb:.0f}MB, 队列大小设置为 {q_size}")
            return q_size, available_mb
            
        except Exception as e:
            print(f"⚠️ 无法获取内存信息: {e}, 使用默认队列大小")
            return os.cpu_count(), np.inf
    
    def check_memory_usage(self, force_check: bool = True) -> Dict:
        """
        检查当前内存使用情况
        """
        try:
            process = psutil.Process(os.getpid())
            memory_info = process.memory_info()
            # rss 表示进程占用的物理内存大小，包括代码、数据、堆栈等
            memory_mb = memory_info.rss / (1024 * 1024)
            
            system_memory = psutil.virtual_memory()
            system_usage_percent = system_memory.percent / 100
            
            memory_status = {
                '[Monitor] RSS Memory': round(memory_mb, 1),
                '[Monitor] Memory Usage': round(system_usage_percent, 1),
                '[Monitor] CPU Usage': round(process.cpu_percent(interval=1.0), 1),
                '[Monitor] Memory Threshold': self.max_mem * self.threshold,
                '[Monitor] Memory Critical': system_usage_percent > self.threshold,
            }
            
            if memory_status['[Monitor] Memory Critical']:
                print(f"🚨 内存使用率{memory_status['[Monitor] Memory Usage']}过高，触发垃圾回收: ")
                print(f"[Monitor] RSS Memory: {memory_status['[Monitor] RSS Memory']} MB")
                print(f"[Monitor] CPU Usage: {memory_status['[Monitor] CPU Usage']}%")

                self._trigger_gc(memory_mb)
            return memory_status
            
        except Exception as e:
            print(f"⚠️ 内存检查失败: {e}")
            return {'error': str(e)}
        
    # def _choose_optimal_generation(self) -> int:
    #     """
    #     选择最优的回收代
    #     策略：
    #     1. 优先回收第0代 新对象 回收效率高
    #     2. 定期回收第1代
    #     3. 少量回收第2代 避免长时间阻塞
    #     """
    #     counts = gc.get_count()
    #     if counts[0] > self.gen0_threshold:  # 📊 第0代对象过多，优先回收
    #         return 0
    #     if counts[1] > self.gen1_threshold:   # 📊 第1代对象积累较多，定期回收
    #         return 1
    #     if counts[2] > self.gen2_threshold or self.processed_count > 1000:  # 📊 第2代对象过多，或处理了大量数据后回收
    #         return 2
    #     return 0  # 默认回收第0代（最轻量）
        
    def _trigger_gc(self, rss_before: float): # 每次触发都是thread
        def async_gc():
            t0 = time.perf_counter()
            gen_count = gc.get_count()
            gc.collect()
            gen_count_after = gc.get_count()
            t1 = time.perf_counter()
            rss_after = psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024)
            print(f"🧹 GC 完成: 用时 {t1 - t0:.2f}s, 释放 {(rss_after - rss_before):.1f} MB, 回收 {np.sum(gen_count) - np.sum(gen_count_after)} 个对象")
        
        print(f"🚨 内存超限，异步触发 GC")
        threading.Thread(target=async_gc, daemon=True).start()

    def get_memory_stats(self) -> Dict:
        """
        获取内存统计信息
        """
        status = self.check_memory_usage(force_check=True)
        
        return {
            'processed_count': self.processed_count,
            'max_memory_mb': self.max_mem,
            **status
        }
    
    def print_stats(self):
        """
        打印内存统计信息
        """
        stats = self.get_memory_stats()
        
        print("\n📊 Graph 内存统计")
        print("=" * 30)
        print(f"处理项目数: {stats['processed_count']:,}")
        print(f"当前内存: {stats.get('process_memory_mb', 0):.1f}MB")
        print(f"内存限制: {stats['max_memory_mb']}MB")
        print(f"系统内存: {stats.get('system_memory_percent', 0):.1f}%")
        
        if stats.get('is_high_usage'):
            print("⚠️ 内存使用偏高")
        elif stats.get('is_critical'):
            print("🚨 内存使用严重过高")
        else:
            print("✅ 内存使用正常")


class GraphMemoryContext:
    """
    Graph 内存管理上下文管理器
    
    使用方式:
    with GraphMemoryContext() as memory_mgr:
        # 你的处理逻辑
        memory_mgr.on_item_processed()
    """
    
    def __init__(self, **kwargs):
        self.manager = GraphMemoryManager()
        # 允许覆盖默认配置
        for key, value in kwargs.items():
            if hasattr(self.manager, key):
                setattr(self.manager, key, value)
    
    def __enter__(self):
        return self.manager
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        # 退出时进行最终清理
        self.manager.force_gc()
        self.manager.print_stats()


#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Graph 内存管理模块

专门为 Graph 类提供内存监控、优化和清理功能
"""
import os
import gc
import psutil
import numpy as np
from typing import Dict, Optional, Tuple


class GraphMemoryManager:
    def __init__(self):
        self.threshold = float(os.getenv("GRAPH_MAX_MEMORY_PERCENT", "75"))
        self.per_item_mb = float(os.getenv("PER_ITEM_SIZE_MB", "500.0"))
        self.memory_check_interval = int(os.getenv("MEMORY_CHECK_INTERVAL", "10"))
        # check threshold
        self.alert_threshold = float(os.getenv("GRAPH_MEM_ALERT_THRESHOLD", "80"))
        self.critical_threshold = float(os.getenv("GRAPH_MEM_CRITICAL_THRESHOLD", "90"))
        
        self.q_size, self.max_mem = self._init()

    def _init(self) -> Tuple[int, float]:
        threshold = float(os.getenv("GRAPH_QSIZE_MEMORY_PERCENT", "75")) / 100
        try:
            # 获取系统内存信息
            memory = psutil.virtual_memory()
            available_mb = threshold * memory.available / (1024 * 1024)
            q_size = max(1, int(available_mb / self.per_item_mb))
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
            process = psutil.Process()
            memory_info = process.memory_info()
            # rss 表示进程占用的物理内存大小，包括代码、数据、堆栈等
            memory_mb = memory_info.rss / (1024 * 1024)
            
            system_memory = psutil.virtual_memory()
            system_usage_percent = system_memory.percent
            
            memory_status = {
                'process_memory_mb': round(memory_mb, 1),
                'system_memory_percent': round(system_usage_percent, 1),
                'is_high_usage': memory_mb > self.max_mem * self.alert_threshold / 100,
                'is_critical': memory_mb > self.max_mem or system_usage_percent > self.critical_threshold
            }
            
            if force_check or memory_status['is_high_usage']:
                print(f"🧠 内存使用: 进程 {memory_status['process_memory_mb']}MB, "
                      f"系统 {memory_status['system_memory_percent']}%")
                
                if memory_status['is_critical']:
                    print("🚨 内存使用率过高，触发垃圾回收")
                    gc.collect()
            
            return memory_status
            
        except Exception as e:
            print(f"⚠️ 内存检查失败: {e}")
            return {'error': str(e)}
   
    def get_memory_stats(self) -> Dict:
        """
        获取内存统计信息
        """
        status = self.check_memory_usage(force_check=True)
        
        return {
            'processed_count': self.processed_count,
            'max_memory_mb': self.max_memory_mb,
            'memory_check_interval': self.memory_check_interval,
            'force_gc_threshold': self.force_gc_threshold,
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


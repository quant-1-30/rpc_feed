#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import gc
import sys
import time
import psutil
import threading
from typing import Dict, List, Optional, Callable, Any
from dataclasses import dataclass
from contextlib import contextmanager


@dataclass
class MemorySnapshot:
    timestamp: float
    process_memory_mb: float
    system_memory_percent: float
    gc_count: tuple
    obj_count: int


class GraphMemoryManager:
    
    def __init__(self):
        self.threshold = float(os.getenv("GRAPH_MAX_MEMORY_PERCENT", "80")) / 100
        self.q_size = int(os.getenv("GRAPH_QSIZE", max(4, os.cpu_count() or 4)))
        self.gc_threads = []

    def check_memory_usage(self) -> Dict[str, Any]:
        """检查 RSS 内存并根据阈值触发 GC"""
        try:
            process = psutil.Process(os.getpid())
            rss_mb = process.memory_info().rss / (1024 * 1024)
            system_memory = psutil.virtual_memory()

            status = {
                'rss_mb': round(rss_mb, 1),
                'usage_percent': system_memory.percent,
                'critical': system_memory.percent > (self.threshold * 100)
            }
            if status['critical']:
                print(f"🚨 [MemoryManager] 内存达到危险水位 ({system_memory.percent}%)，触发异步 GC")
                self._trigger_gc(rss_mb)
            return status
        except Exception as e:
            return {'error': str(e)}

    def _trigger_gc(self, rss_before: float):
        def async_gc():
            t0 = time.perf_counter()
            objs_before = gc.get_count()
            gc.collect()
            t1 = time.perf_counter()
            rss_after = psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024)
            print(f"🧹 [GC] 完成: 耗时 {t1-t0:.2f}s, 释放约 {rss_before - rss_after:.1f} MB")

        thd = threading.Thread(target=async_gc, daemon=True)
        thd.start()
        self.gc_threads.append(thd)

    def cleanup_gc(self):
        for t in self.gc_threads:
            if t.is_alive(): t.join(timeout=1.0)
        self.gc_threads.clear()


class MemoryMonitor:
    
    def __init__(self, 
                 max_memory_mb: int = 2048,
                 warning_threshold: float = 0.8,
                 critical_threshold: float = 0.9,
                 monitor_interval: float = 10.0):
        self.max_memory_mb = max_memory_mb
        self.warning_threshold = warning_threshold
        self.critical_threshold = critical_threshold
        self.monitor_interval = monitor_interval
        
        self.snapshots: List[MemorySnapshot] = []
        self.alerts_sent = set()
        self.monitoring = False
        self.monitor_thread = None
        
        self.warning_callback: Optional[Callable] = None
        self.critical_callback: Optional[Callable] = None
    
    def get_memory_info(self) -> MemorySnapshot:
        """获取当前内存信息"""
        try:
            process = psutil.Process()
            memory_info = process.memory_info()
            
            system_memory = psutil.virtual_memory()
            
            return MemorySnapshot(
                timestamp=time.time(),
                process_memory_mb=memory_info.rss / (1024 * 1024),
                system_memory_percent=system_memory.percent,
                gc_count=gc.get_count(),
                obj_count=len(gc.get_objects())
            )
        except Exception as e:
            print(f"❌ 获取内存信息失败: {e}")
            return None
    
    def check_memory_status(self, snapshot: MemorySnapshot = None) -> Dict:
        """检查内存状态"""
        if snapshot is None:
            snapshot = self.get_memory_info()
        
        if not snapshot:
            return {'status': 'error', 'message': '无法获取内存信息'}
        
        memory_usage_ratio = snapshot.process_memory_mb / self.max_memory_mb
        
        status = {
            'timestamp': snapshot.timestamp,
            'process_memory_mb': round(snapshot.process_memory_mb, 1),
            'system_memory_percent': round(snapshot.system_memory_percent, 1),
            'memory_usage_ratio': round(memory_usage_ratio, 3),
            'gc_count': snapshot.gc_count,
            'obj_count': snapshot.obj_count,
            'is_warning': memory_usage_ratio >= self.warning_threshold,
            'is_critical': memory_usage_ratio >= self.critical_threshold,
            'status': 'normal'
        }
        
        if status['is_critical']:
            status['status'] = 'critical'
            status['message'] = f"🚨 内存使用严重过高: {status['process_memory_mb']}MB"
        elif status['is_warning']:
            status['status'] = 'warning'
            status['message'] = f"⚠️ 内存使用偏高: {status['process_memory_mb']}MB"
        else:
            status['message'] = f"✅ 内存使用正常: {status['process_memory_mb']}MB"
        
        return status
    
    def force_cleanup(self) -> Dict:
        """强制内存清理"""
        print("🧹 开始强制内存清理...")
        
        before_snapshot = self.get_memory_info()
        
        # 多轮垃圾回收
        collected_objects = 0
        for i in range(3):
            collected = gc.collect()
            collected_objects += collected
            time.sleep(0.1)  # 给系统一点时间
        
        after_snapshot = self.get_memory_info()
        
        if before_snapshot and after_snapshot:
            freed_mb = before_snapshot.process_memory_mb - after_snapshot.process_memory_mb
            result = {
                'collected_objects': collected_objects,
                'freed_memory_mb': round(freed_mb, 2),
                'before_memory_mb': round(before_snapshot.process_memory_mb, 1),
                'after_memory_mb': round(after_snapshot.process_memory_mb, 1)
            }
            
            print(f"🗑️ 垃圾回收完成: 清理 {collected_objects} 对象, "
                  f"释放 {result['freed_memory_mb']}MB")
            
            return result
        else:
            return {'error': '无法获取清理前后的内存信息'}
    
    def start_monitoring(self):
        """开始后台内存监控"""
        if self.monitoring:
            print("⚠️ 内存监控已经在运行")
            return
        
        self.monitoring = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()
        print(f"🔍 内存监控已启动 (间隔: {self.monitor_interval}s)")
    
    def stop_monitoring(self):
        """停止内存监控"""
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5)
        print("🛑 内存监控已停止")
    
    def _monitor_loop(self):
        """监控循环"""
        while self.monitoring:
            try:
                snapshot = self.get_memory_info()
                if snapshot:
                    self.snapshots.append(snapshot)
                    
                    # 保留最近100个快照
                    if len(self.snapshots) > 100:
                        self.snapshots = self.snapshots[-100:]
                    
                    status = self.check_memory_status(snapshot)
                    
                    # 触发警告回调
                    if status['is_warning'] and 'warning' not in self.alerts_sent:
                        self.alerts_sent.add('warning')
                        if self.warning_callback:
                            self.warning_callback(status)
                        else:
                            print(status['message'])
                    
                    # 触发严重警告回调
                    if status['is_critical'] and 'critical' not in self.alerts_sent:
                        self.alerts_sent.add('critical')
                        if self.critical_callback:
                            self.critical_callback(status)
                        else:
                            print(status['message'])
                            self.force_cleanup()
                    
                    # 如果内存使用下降，重置警告
                    if not status['is_warning']:
                        self.alerts_sent.discard('warning')
                    if not status['is_critical']:
                        self.alerts_sent.discard('critical')
                
                time.sleep(self.monitor_interval)
                
            except Exception as e:
                print(f"❌ 监控循环错误: {e}")
                time.sleep(self.monitor_interval)
    
    def get_memory_trend(self, recent_minutes: int = 5) -> Dict:
        """分析最近的内存使用趋势"""
        if not self.snapshots:
            return {'error': '没有足够的历史数据'}
        
        cutoff_time = time.time() - (recent_minutes * 60)
        recent_snapshots = [s for s in self.snapshots if s.timestamp >= cutoff_time]
        
        if len(recent_snapshots) < 2:
            return {'error': '没有足够的最近数据'}
        
        first = recent_snapshots[0]
        last = recent_snapshots[-1]
        
        memory_change = last.process_memory_mb - first.process_memory_mb
        time_span = last.timestamp - first.timestamp
        
        trend = {
            'time_span_minutes': round(time_span / 60, 1),
            'memory_change_mb': round(memory_change, 2),
            'rate_mb_per_minute': round(memory_change / (time_span / 60), 2) if time_span > 0 else 0,
            'obj_count_change': last.obj_count - first.obj_count,
            'trend': 'increasing' if memory_change > 0 else 'decreasing' if memory_change < 0 else 'stable'
        }
        
        # 预测
        if trend['rate_mb_per_minute'] > 0:
            remaining_memory = self.max_memory_mb - last.process_memory_mb
            if remaining_memory > 0:
                time_to_limit = remaining_memory / trend['rate_mb_per_minute']
                trend['estimated_minutes_to_limit'] = round(time_to_limit, 1)
        
        return trend
    
    def print_summary(self):
        """打印内存使用摘要"""
        status = self.check_memory_status()
        trend = self.get_memory_trend()
        
        print("\n📊 内存使用摘要")
        print("=" * 40)
        print(f"当前状态: {status.get('message', 'N/A')}")
        print(f"进程内存: {status.get('process_memory_mb', 0):.1f}MB")
        print(f"系统内存: {status.get('system_memory_percent', 0):.1f}%")
        print(f"内存限制: {self.max_memory_mb}MB")
        print(f"对象数量: {status.get('obj_count', 0):,}")
        
        if not trend.get('error'):
            print(f"\n📈 内存趋势 (最近 {trend.get('time_span_minutes', 0):.1f} 分钟):")
            print(f"变化量: {trend.get('memory_change_mb', 0):+.2f}MB")
            print(f"变化率: {trend.get('rate_mb_per_minute', 0):+.2f}MB/分钟")
            print(f"趋势: {trend.get('trend', 'unknown')}")
            
            if 'estimated_minutes_to_limit' in trend:
                print(f"⚠️ 预计 {trend['estimated_minutes_to_limit']:.1f} 分钟后达到内存限制")


@contextmanager
def memory_limit(max_memory_mb: int = 2048, auto_cleanup: bool = True):
    """
    with memory_limit(1024):  # 限制1GB
        *****
    """
    monitor = MemoryMonitor(max_memory_mb=max_memory_mb)
    
    if auto_cleanup:
        def cleanup_callback(status):
            print(f"🚨 内存使用过高，自动清理: {status['process_memory_mb']}MB")
            monitor.force_cleanup()
        
        monitor.critical_callback = cleanup_callback
    
    monitor.start_monitoring()
    
    try:
        yield monitor
    finally:
        monitor.stop_monitoring()
        if auto_cleanup:
            monitor.force_cleanup()


def diagnose_memory_issue() -> Dict:
    print("=" * 30)
    try:
        process = psutil.Process()
        memory_info = process.memory_info()
        system_memory = psutil.virtual_memory()
        
        # 垃圾回收信息
        gc_stats = gc.get_stats()
        gc_counts = gc.get_count()
        
        diagnosis = {
            'process_memory_mb': round(memory_info.rss / (1024 * 1024), 1),
            'system_available_mb': round(system_memory.available / (1024 * 1024), 1),
            'system_used_percent': round(system_memory.percent, 1),
            'gc_counts': gc_counts,
            'gc_stats': gc_stats,
            'obj_count': len(gc.get_objects())
        }
        
        print(f"进程内存使用: {diagnosis['process_memory_mb']}MB")
        print(f"系统可用内存: {diagnosis['system_available_mb']}MB")
        print(f"系统内存使用率: {diagnosis['system_used_percent']}%")
        print(f"Python 对象数量: {diagnosis['obj_count']:,}")
        print(f"垃圾回收计数: {diagnosis['gc_counts']}")
        
        # 建议
        suggestions = []
        
        if diagnosis['process_memory_mb'] > 1000:
            suggestions.append("进程内存使用较高，考虑分批处理数据")
        
        if diagnosis['system_used_percent'] > 80:
            suggestions.append("系统内存使用率过高，关闭其他程序或增加内存")
        
        if diagnosis['obj_count'] > 100000:
            suggestions.append("Python 对象数量很多，建议强制垃圾回收")
        
        if suggestions:
            print(f"\n💡 建议:")
            for i, suggestion in enumerate(suggestions, 1):
                print(f"  {i}. {suggestion}")
        
        return diagnosis
        
    except Exception as e:
        print(f"❌ 诊断失败: {e}")
        return {'error': str(e)}

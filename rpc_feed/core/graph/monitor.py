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


from rpc_feed.utils.wrapper import env_float

@dataclass
class MemorySnapshot:
    timestamp: float
    process_memory_mb: float
    system_memory_percent: float
    gc_count: tuple
    gc_count_total: int


class GraphMemoryManager:
    
    def __init__(self):
        self.threshold = env_float("GRAPH_MAX_MEMORY_PERCENT", 80.0) / 100
        self.gc_threads = []

    def check_memory_usage(self) -> Dict[str, Any]:
        """ RSS Mem and Trigger gc by threshold"""
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
                print(f"🚨 [MemoryManager] HWM ({system_memory.percent}%), Trigger GC")
                self._trigger_gc(rss_mb)
            return status
        except Exception as e:
            return {'error': str(e)}

    def _trigger_gc(self, rss_before: float):
        def async_gc():
            t0 = time.perf_counter()
            gc.collect()
            t1 = time.perf_counter()
            rss_after = psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024)
            print(f"🧹 [GC] Elapsed: {t1-t0:.2f}s, Released: {rss_before - rss_after:.1f} MB")

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
        """obtain current memory information"""
        try:
            process = psutil.Process()
            memory_info = process.memory_info()
            
            system_memory = psutil.virtual_memory()
            
            gc_counts = gc.get_count()
            return MemorySnapshot(
                timestamp=time.time(),
                process_memory_mb=memory_info.rss / (1024 * 1024),
                system_memory_percent=system_memory.percent,
                gc_count=gc_counts,
                gc_count_total=sum(gc_counts),
            )
        except Exception as e:
            print(f"❌: {e}")
            return None
    
    def check_memory_status(self, snapshot: MemorySnapshot = None) -> Dict:
        """Check memory status based on a MemorySnapshot"""
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
            'gc_count_total': snapshot.gc_count_total,
            'is_warning': memory_usage_ratio >= self.warning_threshold,
            'is_critical': memory_usage_ratio >= self.critical_threshold,
            'status': 'normal'
        }
        
        if status['is_critical']:
            status['status'] = 'critical'
            status['message'] = f"🚨 内存使用严重过高: {status['process_memory_mb']}MB"
        elif status['is_warning']:
            status['status'] = 'warning'
            status['message'] = f"⚠️ Mem HWM: {status['process_memory_mb']}MB"
        else:
            status['message'] = f"✅ Mem Normal: {status['process_memory_mb']}MB"
        
        return status
    
    def force_cleanup(self) -> Dict:
        """gc.collect() and return memory info before and after"""
        print("🧹 force cleanup...")
        
        before_snapshot = self.get_memory_info()
        
        # recycle 3 times to ensure thorough cleanup
        collected_objects = 0
        for i in range(3):
            collected = gc.collect()
            collected_objects += collected
            time.sleep(0.1) # buffer 
        
        after_snapshot = self.get_memory_info()
        
        if before_snapshot and after_snapshot:
            freed_mb = before_snapshot.process_memory_mb - after_snapshot.process_memory_mb
            result = {
                'collected_objects': collected_objects,
                'freed_memory_mb': round(freed_mb, 2),
                'before_memory_mb': round(before_snapshot.process_memory_mb, 1),
                'after_memory_mb': round(after_snapshot.process_memory_mb, 1)
            }
            
            print(f"🗑️ cleanup {collected_objects} objects, "
                  f"released {result['freed_memory_mb']}MB")
            
            return result
        else:
            return {'error': 'no memory info available'}
    
    def start_monitoring(self):
        """Start memory monitoring in a separate thread"""
        if self.monitoring:
            return
        
        self.monitoring = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()
        print(f"🔍 Memory monitoring started (interval: {self.monitor_interval}s)")
    
    def stop_monitoring(self):
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5)
    
    def _monitor_loop(self):
        """monitor loop"""
        while self.monitoring:
            try:
                snapshot = self.get_memory_info()
                if snapshot:
                    self.snapshots.append(snapshot)
                    
                    if len(self.snapshots) > 100:
                        self.snapshots = self.snapshots[-100:]
                    
                    status = self.check_memory_status(snapshot)
                    
                    if status['is_warning'] and 'warning' not in self.alerts_sent:
                        self.alerts_sent.add('warning')
                        if self.warning_callback:
                            self.warning_callback(status)
                        else:
                            print(status['message'])
                    
                    if status['is_critical'] and 'critical' not in self.alerts_sent:
                        self.alerts_sent.add('critical')
                        if self.critical_callback:
                            self.critical_callback(status)
                        else:
                            print(status['message'])
                            self.force_cleanup()
                    
                    if not status['is_warning']:
                        self.alerts_sent.discard('warning')
                    if not status['is_critical']:
                        self.alerts_sent.discard('critical')
                
                time.sleep(self.monitor_interval)
                
            except Exception as e:
                print(f"❌ Monitor loop error: {e}")
                time.sleep(self.monitor_interval)
    
    def get_memory_trend(self, recent_minutes: int = 5) -> Dict:
        """Analyze the recent memory usage trend"""
        if not self.snapshots:
            return {'error': 'no sufficient historical data'}
        
        cutoff_time = time.time() - (recent_minutes * 60)
        recent_snapshots = [s for s in self.snapshots if s.timestamp >= cutoff_time]
        
        if len(recent_snapshots) < 2:
            return {'error': 'no sufficient recent data'}
        
        first = recent_snapshots[0]
        last = recent_snapshots[-1]
        
        memory_change = last.process_memory_mb - first.process_memory_mb
        time_span = last.timestamp - first.timestamp
        
        trend = {
            'time_span_minutes': round(time_span / 60, 1),
            'memory_change_mb': round(memory_change, 2),
            'rate_mb_per_minute': round(memory_change / (time_span / 60), 2) if time_span > 0 else 0,
            'gc_count_total_change': last.gc_count_total - first.gc_count_total,
            'trend': 'increasing' if memory_change > 0 else 'decreasing' if memory_change < 0 else 'stable'
        }
        
        # predict time to reach max memory if increasing
        if trend['rate_mb_per_minute'] > 0:
            remaining_memory = self.max_memory_mb - last.process_memory_mb
            if remaining_memory > 0:
                time_to_limit = remaining_memory / trend['rate_mb_per_minute']
                trend['estimated_minutes_to_limit'] = round(time_to_limit, 1)
        
        return trend
    
    def print_summary(self):
        """Print memory usage summary"""
        status = self.check_memory_status()
        trend = self.get_memory_trend()
        
        print("\n📊 Memory Usage Summary")
        print("=" * 40)
        print(f"Current Status: {status.get('message', 'N/A')}")
        print(f"Process Memory: {status.get('process_memory_mb', 0):.1f}MB")
        print(f"System Memory: {status.get('system_memory_percent', 0):.1f}%")
        print(f"Memory Limit: {self.max_memory_mb}MB")
        print(f"GC Count Total: {status.get('gc_count_total', 0):,}")
        
        if not trend.get('error'):
            print(f"\n📈 Memory Trend (Recent {trend.get('time_span_minutes', 0):.1f} Minutes):")
            print(f"Change: {trend.get('memory_change_mb', 0):+.2f}MB")
            print(f"Rate: {trend.get('rate_mb_per_minute', 0):+.2f}MB/Minute")
            print(f"Trend: {trend.get('trend', 'unknown')}")
            
            if 'estimated_minutes_to_limit' in trend:
                print(f"⚠️ Estimated {trend['estimated_minutes_to_limit']:.1f} Minutes to Reach Memory Limit")


@contextmanager
def memory_limit(max_memory_mb: int = 2048, auto_cleanup: bool = True):
    """
    with memory_limit(1024):  # limit to 1GB
        *****
    """
    monitor = MemoryMonitor(max_memory_mb=max_memory_mb)
    
    if auto_cleanup:
        def cleanup_callback(status):
            print(f"🚨 Memory usage too high, auto-cleaning: {status['process_memory_mb']}MB")
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
        
        gc_stats = gc.get_stats()
        gc_counts = gc.get_count()
        
        diagnosis = {
            'process_memory_mb': round(memory_info.rss / (1024 * 1024), 1),
            'system_available_mb': round(system_memory.available / (1024 * 1024), 1),
            'system_used_percent': round(system_memory.percent, 1),
            'gc_counts': gc_counts,
            'gc_stats': gc_stats,
            'gc_count_total': sum(gc_counts),
        }
        
        print(f"Process Memory Usage: {diagnosis['process_memory_mb']}MB")
        print(f"System Available Memory: {diagnosis['system_available_mb']}MB")
        print(f"System Memory Usage: {diagnosis['system_used_percent']}%")
        print(f"GC Count Total: {diagnosis['gc_count_total']:,}")
        print(f"Garbage Collection Counts: {diagnosis['gc_counts']}")
        
        # Suggestions
        suggestions = []
        
        if diagnosis['process_memory_mb'] > 1000:
            suggestions.append("Process memory usage is high, consider processing data in batches")
        
        if diagnosis['system_used_percent'] > 80:
            suggestions.append("System memory usage is high, close other programs or add more memory")
        
        if diagnosis['gc_count_total'] > 100000:
            suggestions.append("GC count is high, consider forcing garbage collection")
        
        if suggestions:
            print(f"\n💡 Suggestions:")
            for i, suggestion in enumerate(suggestions, 1):
                print(f"  {i}. {suggestion}")
        
        return diagnosis
        
    except Exception as e:
        print(f"❌ Diagnosis failed: {e}")
        return {'error': str(e)}

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import signal
import platform
import multiprocessing


def fix_macos_mp(force: bool = True) -> bool:
    """
    修复 macOS multiprocessing 兼容性修复工具
        - OSError: [Errno 6] Device not configured
        - fork() 安全性问题  
        - pandas/numpy 在子进程中的兼容性问题
        - fork 与 spawn 区别 --- fork 复制进程副本快速启动无需导入模块 unix/ linux | spawn 启动python解释器需要导入模块隔离性好所有平台都可用
        
    Args:
        force: 是否强制设置 (默认 True)
        
    Returns:
        bool: 是否成功设置
        
    关于 force 参数:
    - force=False: 如果已经设置过会抛出 RuntimeError
    - force=True: 可以覆盖已经设置的方法 (推荐)
    """
    if platform.system() != "Darwin":
        print("ℹ️  非 macOS 系统，跳过 multiprocessing 修复")
        return True
    
    try:
        # 获取当前方法
        current_method = multiprocessing.get_start_method(allow_none=True)
        print(f"🔍 当前 multiprocessing 方法: {current_method}")
        
        if current_method == 'spawn':
            print(f"✅ 已经是 spawn 方法，无需修改")
            return True
        
        # 设置为 spawn 方法 force=True 是关键！否则会报错 RuntimeError: context has already been set
        multiprocessing.set_start_method('spawn', force=force)
        print("🔧 已设置 multiprocessing 为 'spawn' 方法")
        return True
        
    except RuntimeError as e:
        if "context has already been set" in str(e):
            print("⚠️  Multiprocessing 上下文已经设置")
            if not force:
                print("💡 尝试使用 force=True 参数")
                return False
            else:
                print("❌ 即使使用 force=True 也无法更改 (可能有进程已启动)")
                return False
        else:
            print(f"❌ 设置失败: {e}")
            return False


def signal_handler(loop):

    def _shutdown_handler(loop):
        print("Signal received, cancelling tasks...")
        for task in asyncio.all_tasks(loop):
            task.cancel()
        
    if platform.system() != "Windows":
        # 🔧 添加 Ctrl+C 捕获
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda: _shutdown_handler(loop))
    else:
        print("Windows system, no signal handler")

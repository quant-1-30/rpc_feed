import os
import glob
import numpy as np
from setuptools import setup, Extension
from Cython.Build import cythonize

# sources = glob.glob("**/*.pyx", recursive=True) # **/*.pyx 会搜索当前目录及其所有子目录下的 .pyx 文件
current_dir = os.path.abspath(os.getcwd())

extensions = [
    Extension(
        name="rpc_feed.core.datasets.provider", 
        sources=["rpc_feed/core/datasets/provider.pyx"],
        include_dirs=[np.get_include(), current_dir],
        language="c++",
        extra_compile_args=["-O3", "-std=c++11"],
        # "-Wno-unused-function",
        # "-Wno-unused-variable",
        # "-Wno-unused-but-set-variable",
        # "-Wno-unused-parameter",
        # "-Wno-sign-compare", # O3 极致优化，C++11 标准
    ),
    Extension(
        name="rpc_feed.core.rpc.feed", 
        sources=["rpc_feed/core/rpc/feed.pyx"],
        include_dirs=[np.get_include(), current_dir],
        language="c++",
        extra_compile_args=["-O3", "-std=c++11"],
    ),
     Extension(
        name="rpc_feed.core.gateway.duckdb.utils", 
        sources=["rpc_feed/core/gateway/duckdb/utils.pyx"],
        include_dirs=[np.get_include(), "."],  
        language="c++",                         
        extra_compile_args=["-O3", "-std=c++11"]
    ),
    Extension(
        name="rpc_feed.utils.dateintern", 
        sources=["rpc_feed/utils/dateintern.pyx"],
        include_dirs=[np.get_include(), current_dir],
        language="c++", # vector/map
        extra_compile_args=["-O3", "-std=c++11"],
        )
]


setup(
    name="rpc_feed_lib",
    ext_modules=cythonize(
        extensions,
        compiler_directives={
            'language_level': "3",       # 使用 Python 3 语法
            'boundscheck': False,        # 关闭数组越界检查（提升性能）
            'wraparound': False,         # 关闭负索引支持（提升性能）
            'initializedcheck': False,   # 关闭内存视图初始化检查
            'cdivision': True,           # 开启 C 级别除法（不检查除零，极快）
        },
        annotate=False # 生成 .html 文件，方便查看代码是否实现C 级加速
    )
)

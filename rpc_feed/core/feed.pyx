#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True

from rpc_feed.core.graph import Graph
from rpc_feed.utils.io import recursive_glob

from rpc_feed.core.datasets import _providers


cdef class BtFeed:

    def __init__(self):
        self._providers = _providers

        self._pattern = {
            ".01":{
                "stock": "^(sh6|sz0|sz3)\d{5}(?:)",
                "fund": "^(sh51|sz15|sz16)\d{4}(?:)",
                "benchmark": "^.+\.01$" # "^[^.]+\.01$" 
            },
            "csv": {
                "stock": "^(SH\.6|SZ\.0|SZ\.3)\d{5}(?:)",
                "fund": "^(SH\.51|SZ\.15|SZ\.16)\d{4}(?:)",
                "benchmark": "^.+\.csv$" 
            }
        }
        
        self.pipeline = Graph()

    # list sids=[] cause share by multi fetch 
    async def fetch(self, str topic, int start_date, int end_date, list sids=None):
        cdef object iterator = self._providers[topic]
        cdef object c_obj
        cdef list sids_arg = sids if sids is not None else []

        async for pb_obj in iterator(start_date, end_date, sids_arg):
            yield pb_obj # protobuf object

    cpdef void load(self, str graph_xml, str dataset_path, str prefix, bint parallel=True) except *: # C无Python异常机制 --- except * Python 异常能被正确捕获和处理非导致程序崩溃或异常丢失
        '''
        Adds a ``Data Feed`` instance to the mix.
        If ``name`` is not None it will be put into ``data._name`` which is
        meant for decoration/plotting purposes.
        '''
        cdef object iterables
        cdef str suffix, sub_suffix

        if "_" not in prefix:
            raise ValueError(f"<suffix>_<sub_suffix>  but got : {prefix!r}")

        suffix, sub_suffix = prefix.split("_", 1)  
        if suffix not in self._pattern or sub_suffix not in self._pattern[suffix]:
            raise KeyError(f"Unkown (suffix, sub_suffix) ({suffix!r}, {sub_suffix!r})")

        iterables = recursive_glob(dataset_path, suffix=suffix, pattern=self._pattern[suffix][sub_suffix])
        self.pipeline.to_execute(graph_xml, iterables, parallel)


bt_feed = BtFeed()

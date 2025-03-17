#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from typing import Dict,Any
from meta import with_metaclass, MetaParams

from core.graph import Graph
from core.datasets import _providers, Request

from filter import _filters
from utils.io import recursive_glob
from utils.loader import get_module_by_module_path
from utils.cache import lazyproperty


class MetaFeed(MetaParams):

    def __donew__(cls, *args, **kwargs):
        _obj, args, kwargs = super().__new__(cls, *args, **kwargs)
        _obj.datesets = _obj._build_dataset()
        # set filter
        _obj.filter = _filters.get(_obj.p._filter)
        # set datasets
        _obj.datasets = _providers
        return _obj, args, kwargs
    
    @staticmethod
    def _build_dataset():
        current_path = os.path.join(os.getcwd(), "core", "datasets.py")
        module = get_module_by_module_path(current_path)
        return module.Providers


class Feed(with_metaclass(MetaFeed, object)):
    
    params = (
        ("filter", "asset"),
        ("graph", Graph())
    )

    def load(self, *args, **kwargs):
        """execute graph to load data"""
        pass

    def next(self, *args, **kwargs):
        """yield data from datasource"""
        pass


class BtFeed(with_metaclass(MetaFeed, object)):


    def load(self, graph_xml, dataset_path, prefix):
        '''
        Adds a ``Data Feed`` instance to the mix.
        If ``name`` is not None it will be put into ``data._name`` which is
        meant for decoration/plotting purposes.
        '''
        glob_paths = recursive_glob(dataset_path, prefix=prefix)
        iterables = self.filter(glob_paths)
        self.pipeline.execute_graph(graph_xml, iterables)

    async def next(self, dataset, request: Request):
         iterator = self.datasets[dataset].load(request)
         async for item in iterator:
             yield item


bt_feed = BtFeed()

__all__ = ["bt_feed"]

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

    def __new__(mcs, name, bases, dct):
        # Ensure 'load' method is implemented
        if 'load' not in dct:
            raise TypeError(f"Class {name} must implement 'load' method")
        # Ensure 'next' method is implemented
        # if 'next' not in dct:
        #     raise TypeError(f"Class {name} must implement 'next' method")
        return super().__new__(mcs, name, bases, dct)

    def donew(cls, *args, **kwargs):
        _obj, args, kwargs = super().donew(*args, **kwargs)
        _obj.datasets = _providers
        _obj.pipeline = Graph()
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

    def donew(cls, *args, **kwargs):
        _obj, args, kwargs = super().donew(*args, **kwargs)
        # set filter
        _obj.filter = _filters.get(_obj.p._filter)
        return _obj, args, kwargs
    
    def load(cls, *args, **kwargs):
        raise NotImplementedError("intend for execute graph")

    def __call__(cls, *args, **kwargs):
        raise NotImplementedError("intend for yield data from datasource")


class BtFeed(with_metaclass(MetaFeed, object)):

    def load(self, graph_xml, dataset_path, prefix):
        '''
        Adds a ``Data Feed`` instance to the mix.
        If ``name`` is not None it will be put into ``data._name`` which is
        meant for decoration/plotting purposes.
        '''
        iterables = recursive_glob(dataset_path, prefix=prefix, filter=self.filter)
        self.pipeline.to_execute(graph_xml, iterables)

    async def __call__(self, dataset, request: Request):
         iterator = self.datasets[dataset](request)
         async for item in iterator:
             yield item


bt_feed = BtFeed()

__all__ = ["bt_feed"]

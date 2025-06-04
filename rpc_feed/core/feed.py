#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os

from .graph import Graph
from .datasets import _providers, Request
from .filter import _filters
from rpc_feed.meta import with_metaclass, MetaParams
from rpc_feed.utils.io import recursive_glob
from rpc_feed.utils.loader import get_module_by_module_path
from rpc_feed.utils.cache import lazyproperty


class FeedMeta(MetaParams):

    # def __new__(meta, name, bases, dct):
    #     # Hack to support original method name for notify_order
    #     if 'notify' in dct:
    #         # rename 'notify' to 'notify_order'
    #         dct['notify_order'] = dct.pop('notify')
    #     if 'notify_operation' in dct:
    #         # rename 'notify' to 'notify_order'
    #         dct['notify_trade'] = dct.pop('notify_operation')
    #     return super(FeedMeta, meta).__new__(meta, name, bases, dct)

    # def __init__(cls, name, bases, dct):
    #     '''
    #     Class has already been created ... register subclasses
    #     '''
    #     # Initialize the class
    #     super(FeedMeta, cls).__init__(name, bases, dct)

    #     if not cls.aliased and \
    #        name != 'Strategy' and not name.startswith('_'):
    #         cls._indcol[name] = cls

    def donew(cls, *args, **kwargs):
        _obj, args, kwargs = super(FeedMeta, cls).donew(*args, **kwargs)
        _obj.datasets = _providers
        _obj.pipeline = Graph()
        # _obj._filter = _filters.get(_obj.p._filter)
        return _obj, args, kwargs
    
    @staticmethod
    def _build_dataset():
        current_path = os.path.join(os.getcwd(), "core", "datasets.py")
        module = get_module_by_module_path(current_path)
        return module.Providers

    def load(cls, *args, **kwargs):
        raise NotImplementedError("intend for execute graph")


class BtFeed(with_metaclass(FeedMeta, object)):

    params = (
        # ("_filter", "null"),
    )

    def load(self, graph_xml, dataset_path, prefix, _filter="null"):
        '''
        Adds a ``Data Feed`` instance to the mix.
        If ``name`` is not None it will be put into ``data._name`` which is
        meant for decoration/plotting purposes.
        '''
        iterables = recursive_glob(dataset_path, suffix=prefix, filter=_filters[_filter])
        self.pipeline.to_execute(graph_xml, iterables)

    async def __call__(self, dataset, request: Request):
         iterator = self.datasets[dataset](request)
         async for item in iterator:
             yield item


bt_feed = BtFeed()

__all__ = ["bt_feed"]

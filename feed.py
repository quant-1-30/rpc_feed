#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
from typing import Dict,Any
from meta import SingletonMeta

from utils.io import recursive_glob
from utils.loader import get_module_by_module_path
from utils.cache import lazyproperty
from core.model import Request
from quandl.graph import Pipeline


class Feed(metaclass=SingletonMeta):

    params = (
        ("alias", "feed"),
        ("filter", True),
        ("pipeline", Pipeline())
    )

    def __init__(self):
        self.providers = self._build_dataset()
    
    @staticmethod
    def _build_dataset():
        current_path = os.path.join(os.getcwd(), "core", "datasets.py")
        module = get_module_by_module_path(current_path)
        return module.Providers

    # @lazyproperty
    # def trading_calendar(self):
    #     return self.provider["calendar"].get_data()
    
    # @lazyproperty
    # def instruments(self):
    #     return self.provider["instrument"].get_data()

    @classmethod
    def filter(cls, paths):
        pattern = "^[6|0|3][0-9]{5}$"
        filtered_paths = []
        for p in paths:
            sid = os.path.basename(p).split('.')[0][2:]
            m = re.match(pattern, sid)
            if m and not m[0].startswith("688"):
                filtered_paths.append(p)
        return filtered_paths
    
    async def replay(self, dataset, request: Request):
         iterator = self.providers[dataset].get_data(request)
         async for item in iterator:
             yield item
    
    def add_data(self, dataset, xml, dataset_path, prefix, filter=False):
        '''
        Adds a ``Data Feed`` instance to the mix.
        If ``name`` is not None it will be put into ``data._name`` which is
        meant for decoration/plotting purposes.
        '''
        glob_paths = recursive_glob(dataset_path, prefix=prefix)
        if filter:
            iterables = self.filter(glob_paths)
        else:
            iterables = glob_paths
        self.pipeline.execute_graph(dataset, xml, iterables)


bt_feed = Feed()

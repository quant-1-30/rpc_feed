#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
from typing import Dict,Any
from meta import SingletonMeta

from utils.io import build_from_cfg, recursive_glob
from utils.loader import get_module_by_module_path
from core.model import Request
from quandl.graph import Pipeline


class Feed(metaclass=SingletonMeta):

    providers = {}
    pipeline = Pipeline()

    def __init__(self):
        self.build_dataset()

    params = (
        ("alias", "feed"),
        ("filter", True)
    )
    
    @property
    def trading_calendar(self):
        return self.provider["calendar"].calendar
    
    @property
    def instruments(self):
        return self.provider["instrument"].instruments

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
    
    @classmethod
    def build_dataset(self):
        current_path = os.path.join(os.getcwd(), "core", "datasets.py")
        module = get_module_by_module_path(current_path)
        self.providers = module.Providers

    async def replay(self, dataset, request: Request):
        return await self.providers[dataset].get_data(request)
    
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

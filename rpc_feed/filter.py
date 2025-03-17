#! /usr/bin/env python3 
# -*- coding: utf-8 -*-

import re

from meta import MetaParams


class Filter(metaclass=MetaParams):
    
    def __donew__(cls, *args, **kwargs):
        _obj, args, kwargs = super().__new__(cls, *args, **kwargs)

        _obj.filter = lambda x: re.match(_obj.p._regex, x)

        return _obj, args, kwargs
    
    def __call__(self, data):
        return [d for d in data if self.filter(d)]


class Nullfilter(Filter):

    params = (("regex", "*"),)

class AssetFilter(Filter):

    params = (("regex", "^[6|0|3][0-9]{5}$"),)


class FundFilter(Filter):

    params = (("regex", "^[51|15]{4}$"),) 


_filters = {
    "null": Nullfilter,
    "asset": AssetFilter,
    "fund": FundFilter,
}

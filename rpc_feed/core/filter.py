#! /usr/bin/env python3 
# -*- coding: utf-8 -*-

import re
from rpc_feed.meta import MetaParams, with_metaclass

__all__ = ["_filters"]


class MetaFilter(MetaParams):
    
    def donew(cls, *args, **kwargs):
        _obj, args, kwargs = super(MetaFilter, cls).donew(*args, **kwargs)
        _obj.filter = lambda x: re.match(_obj.p.regex, x)
        return _obj, args, kwargs
    
    
class Filter(with_metaclass(MetaFilter, object)):

    def __call__(self, meta):
        return True if self.filter(meta) else False


class Nullfilter(Filter):

    params = (("regex", ".*"),)


class AssetFilter(Filter):

    # params = (("regex", "^(sh6|sz0|sz3)\d{5}(?:)"),)       # struct
    params = (("regex", "^(SH\.6|SZ\.0|SZ\.3)\d{5}(?:)"),)   # csv


class FundFilter(Filter):

    # params = (("regex", "^(sh51|sz15|sz16)\d{4}(?:)"),) # sz16 --- lof(场内与场外) 每日更新 / etf 15s update 场内 
    params = (("regex", "^(SH\.51|SZ\.15|SZ\.16)\d{4}(?:)"),) # csv 


_filters = {
    "null": Nullfilter(),
    "asset": AssetFilter(),
    "fund": FundFilter(),
}

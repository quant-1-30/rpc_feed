#! /usr/bin/env python3 
# -*- coding: utf-8 -*-

import re
from typing import List, Any
from rpc_feed.meta import MetaParams, with_metaclass


class MetaFilter(MetaParams):
    
    def donew(cls, *args, **kwargs):
        _obj, args, kwargs = super(MetaFilter, cls).donew(*args, **kwargs)
        _obj.filter = lambda x: re.match(_obj.p.regex, x).group()
        return _obj, args, kwargs
    
    
class Filter(with_metaclass(MetaFilter, object)):

    def __call__(self, meta):
        return True if self.filter(meta) else False


class Nullfilter(Filter):

    params = (("regex", ".*"),)

class AssetFilter(Filter):

    params = (("regex", "^[6|0|3][0-9]{5}$"),)


class FundFilter(Filter):

    params = (("regex", "^[51|15]{4}$"),) 


_filters = {
    "null": Nullfilter(),
    "asset": AssetFilter(),
    "fund": FundFilter(),
}

#! /usr/bin/env python3 
# -*- coding: utf-8 -*-

import re
from rpc_feed.meta import MetaParams, with_metaclass


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

    params = (("regex", "^(sh|sz)(6|0|3)\d{5}(?:)"),)


class FundFilter(Filter):

    params = (("regex", "^(51|15)\d{4}(?:)"),) 


_filters = {
    "null": Nullfilter(),
    "asset": AssetFilter(),
    "fund": FundFilter(),
}

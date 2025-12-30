#! /usr/bin/env python3 
# -*- coding: utf-8 -*-

from core.graph.meta import MetaParams, with_metaclass


class MetaNode(MetaParams):

    def donew(cls, *args, **kwargs):

        _obj, args, kwargs = super(MetaNode, cls).donew(*args, **kwargs)

        # ownerskip = kwargs.pop('_ownerskip', None)
        # _obj._owner = findowner(_obj, _obj._OwnerCls or LineMultiple, skip=ownerskip)

        alias = kwargs.pop('alias', cls.__name__)
        _obj.alias = alias

        if not hasattr(_obj, "prenext"):
            _obj.prenext = lambda: None

        if not hasattr(_obj, "next"):
            raise NotImplementedError("next")

        return _obj, args, kwargs
    
    def __call__(cls, *args, **kwargs):
        return super().__call__(*args, **kwargs)
    

class Node(with_metaclass(MetaNode, object)):
    # metabase __call__ include doinit ，so just use Node() will automate replace params

    params = (
        ("refname", None),
        ("binds", []),
        ("is_writer", False)
    )

    def prenext(self, *args, **kwargs):
        '''
        It will be called before next 
        '''
        pass

    def next(self, *args, **kwargs):
        '''
        Called to calculate values when graph is executed
        '''
        pass

    def __neg__(self):
        self.next = lambda x: x
        return self
    
    def __eq__(self, other):
        is_same = self.next == other.next
        return is_same
    
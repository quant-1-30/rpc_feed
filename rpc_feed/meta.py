# /usr/bin/env python3
# -*- coding: utf-8 -*-
import re
import sys
import itertools
import logging
from typing import Any, List
from collections import OrderedDict


# This is from Armin Ronacher from Flash simplified later by six
def with_metaclass(meta, *bases):
    """Create a base class with a metaclass."""
    # This requires a bit of explanation: the basic idea is to make a dummy
    # metaclass for one level of class instantiation that replaces itself with
    # the actual metaclass.
    class metaclass(meta):

        def __new__(cls, name, this_bases, d):
            return meta(name, bases, d)
    return type.__new__(metaclass, str('temporary_class'), (), {})


def findbases(kls, topclass):
    retval = list()
    for base in kls.__bases__:
        if issubclass(base, topclass):
            retval.extend(findbases(base, topclass))
            retval.append(base)

    return retval


def findowner(owned, cls, startlevel=2, skip=None):
    # skip this frame and the caller's -> start at 2
    for framelevel in itertools.count(startlevel):
        try:
            frame = sys._getframe(framelevel)
        except ValueError:
            # Frame depth exceeded ... no owner ... break away
            break

        # 'self' in regular code
        self_ = frame.f_locals.get('self', None)
        if skip is not self_:
            if self_ is not owned and isinstance(self_, cls):
                return self_

        # '_obj' in metaclasses
        obj_ = frame.f_locals.get('_obj', None)
        if skip is not obj_:
            if obj_ is not owned and isinstance(obj_, cls):
                return obj_

    return None


class MetaBase(type):
        # 类实例属性赋值调用__setattr__ --- 负责在__dict__注册; 所以重载__setattr__注意, 要不手动注册__dict__
        # clsmodule = sys.modules[cls.__module__]
        # cls.__name__ 为 MetaBase ; name为子类
        # type 与 type.__new__ 区别前者生产新类, 后者实例自动触发init
        # newcls = type(newclsname, bases, attrs)
    def doprenew(cls, *args, **kwargs):
        return cls, args, kwargs

    def donew(cls, *args, **kwargs):
        # super().__new__（cls， *args, **kwargs) type OK / 如果只是object不能有其他参数 
        _obj = cls.__new__(cls, *args, **kwargs)
        return _obj, args, kwargs

    def dopreinit(cls, _obj, *args, **kwargs):
        return _obj, args, kwargs

    def doinit(cls, _obj, *args, **kwargs):
        _obj.__init__(*args, **kwargs)
        return _obj, args, kwargs

    def dopostinit(cls, _obj, *args, **kwargs):
        return _obj, args, kwargs

    def __call__(cls, *args, **kwargs):
        # # python 对象创建两种方式 Python/C Api ; 调用对象. 如果实例对象需要定义__call__, 如果
        # def __call__(cls, *args: Any, **kwds: Any) -> Any:
        #     print("entering into metabase __call__", args, kwds)
        #     return super().__call__(*args, **kwds)
        cls, args, kwargs = cls.doprenew(*args, **kwargs)
        _obj, args, kwargs = cls.donew(*args, **kwargs)
        _obj, args, kwargs = cls.dopreinit(_obj, *args, **kwargs)
        _obj, args, kwargs = cls.doinit(_obj, *args, **kwargs)
        _obj, args, kwargs = cls.dopostinit(_obj, *args, **kwargs)
        return _obj


class AutoInfoClass(object):
    _getpairsbase = classmethod(lambda cls: OrderedDict())
    _getpairs = classmethod(lambda cls: OrderedDict())
    _getrecurse = classmethod(lambda cls: False)

    @classmethod
    def _derive(cls, name, info, otherbases, recurse=False):
        # collect the 3 set of infos
        # info = OrderedDict(info)
        baseinfo = cls._getpairs().copy()
        obasesinfo = OrderedDict()
        for obase in otherbases:
            if isinstance(obase, (tuple, dict)):
                obasesinfo.update(obase)
            else:
                obasesinfo.update(obase._getpairs())

        # update the info of this class (base) with that from the other bases
        baseinfo.update(obasesinfo)

        # The info of the new class is a copy of the full base info
        # plus and update from parameter
        clsinfo = baseinfo.copy()
        clsinfo.update(info)

        # The new items to update/set are those from the otherbase plus the new
        info2add = obasesinfo.copy()
        info2add.update(info)

        clsmodule = sys.modules[cls.__module__]
        newclsname = str(cls.__name__ + '_' + name)  # str - Python 2/3 compat

        # This loop makes sure that if the name has already been defined, a new
        # unique name is found. A collision example is in the plotlines names
        # definitions of bt.indicators.MACD and bt.talib.MACD. Both end up
        # definining a MACD_pl_macd and this makes it impossible for the pickle
        # module to send results over a multiprocessing channel
        namecounter = 1
        while hasattr(clsmodule, newclsname):
            newclsname += str(namecounter)
            namecounter += 1

        newcls = type(newclsname, (cls,), {})
        setattr(clsmodule, newclsname, newcls)

        setattr(newcls, '_getpairsbase',
                classmethod(lambda cls: baseinfo.copy()))
        setattr(newcls, '_getpairs', classmethod(lambda cls: clsinfo.copy()))
        setattr(newcls, '_getrecurse', classmethod(lambda cls: recurse))

        for infoname, infoval in info2add.items():
            if recurse:
                recursecls = getattr(newcls, infoname, AutoInfoClass)
                infoval = recursecls._derive(name + '_' + infoname,
                                             infoval,
                                             [])

            setattr(newcls, infoname, infoval)

        return newcls

    def isdefault(self, pname):
        return self._get(pname) == self._getkwargsdefault()[pname]

    def notdefault(self, pname):
        return self._get(pname) != self._getkwargsdefault()[pname]

    def _get(self, name, default=None):
        return getattr(self, name, default)

    @classmethod
    def _getkwargsdefault(cls):
        return cls._getpairs()

    @classmethod
    def _getkeys(cls):
        return cls._getpairs().keys()

    @classmethod
    def _getdefaults(cls):
        return list(cls._getpairs().values())

    @classmethod
    def _getitems(cls):
        return cls._getpairs().items()

    @classmethod
    def _gettuple(cls):
        return tuple(cls._getpairs().items())

    def _getkwargs(self, skip_=False):
        l = [
            (x, getattr(self, x))
            for x in self._getkeys() if not skip_ or not x.startswith('_')]
        return OrderedDict(l)

    def _getvalues(self):
        return [getattr(self, x) for x in self._getkeys()]

    def __new__(cls, *args, **kwargs):
        obj = super(AutoInfoClass, cls).__new__(cls, *args, **kwargs)

        if cls._getrecurse():
            for infoname in obj._getkeys():
                recursecls = getattr(cls, infoname)
                setattr(obj, infoname, recursecls())

        return obj


class MetaParams(MetaBase):
    def __new__(meta, name, bases, dct):
        # Remove params from class definition to avoid inheritance
        # (and hence "repetition")
        newparams = dct.pop('params', ())

        # packs = 'packages'
        # newpackages = tuple(dct.pop(packs, ()))  # remove before creation

        # fpacks = 'frompackages'
        # fnewpackages = tuple(dct.pop(fpacks, ()))  # remove before creation

        # Create the new class - this pulls predefined "params"
        cls = super(MetaParams, meta).__new__(meta, name, bases, dct)

        # Pulls the param class out of it - default is the empty class
        # MetaParams must have not params / method is to transform every base class params to AutoInfoClass
        # and via _derive to compose all class params to construct a new class with the params
        params = getattr(cls, 'params', AutoInfoClass)

        # Pulls the packages class out of it - default is the empty class
        # packages = tuple(getattr(cls, packs, ()))
        # fpackages = tuple(getattr(cls, fpacks, ()))

        # get extra (to the right) base classes which have a param attribute
        morebasesparams = [x.params for x in bases[1:] if hasattr(x, 'params')]

        # # Get extra packages, add them to the packages and put all in the class
        # for y in [x.packages for x in bases[1:] if hasattr(x, packs)]:
        #     packages += tuple(y)

        # for y in [x.frompackages for x in bases[1:] if hasattr(x, fpacks)]:
        #     fpackages += tuple(y)

        # cls.packages = packages + newpackages
        # cls.frompackages = fpackages + fnewpackages

        # Subclass and store the newly derived params class
        cls.params = params._derive(name, newparams, morebasesparams)

        return cls

    def donew(cls, *args, **kwargs):
        clsmod = sys.modules[cls.__module__]
        # import specified packages
        # for p in cls.packages:
        #     if isinstance(p, (tuple, list)):
        #         p, palias = p
        #     else:
        #         palias = p

        #     pmod = __import__(p)

        #     plevels = p.split('.')
        #     if p == palias and len(plevels) > 1:  # 'os.path' not aliased
        #         setattr(clsmod, pmod.__name__, pmod)  # set 'os' in module

        #     else:  # aliased and/or dots
        #         for plevel in plevels[1:]:  # recurse down the mod
        #             pmod = getattr(pmod, plevel)

        #         setattr(clsmod, palias, pmod)

        # # import from specified packages - the 2nd part is a string or iterable
        # for p, frompackage in cls.frompackages:
        #     if isinstance(frompackage, str):
        #         frompackage = (frompackage,)  # make it a tuple

        #     for fp in frompackage:
        #         if isinstance(fp, (tuple, list)):
        #             fp, falias = fp
        #         else:
        #             fp, falias = fp, fp  # assumed is string

        #         # complain "not string" without fp (unicode vs bytes)
        #         pmod = __import__(p, fromlist=[str(fp)])
        #         pattr = getattr(pmod, fp)
        #         setattr(clsmod, falias, pattr)
        #         for basecls in cls.__bases__:
        #             setattr(sys.modules[basecls.__module__], falias, pattr)

        # Create params and set the values from the kwargs
        params = cls.params()
        for pname, pdef in cls.params._getitems():
            setattr(params, pname, kwargs.pop(pname, pdef))

        # Create the object and set the params in place
        _obj, args, kwargs = super(MetaParams, cls).donew(*args, **kwargs)
        _obj.params = params
        _obj.p = params  # shorter alias

        # Parameter values have now been set before __init__
        return _obj, args, kwargs


class MetaSingleton(MetaParams):
    '''Metaclass to make a metaclassed class a singleton'''
    def __init__(cls, name, bases, dct):
        super(MetaSingleton, cls).__init__(name, bases, dct)
        cls._singleton = None

    def __call__(cls, *args, **kwargs):
        if cls._singleton is None:
            cls._singleton = (
                super(MetaSingleton, cls).__call__(*args, **kwargs))

        return cls._singleton
    

class ParamsBase(with_metaclass(MetaParams, object)):
    pass  # stub to allow easy subclassing without metaclasses


# class SingletonMeta(type):

#     _instances = {}

#     def __call__(cls, *args: Any, **kwds: Any) -> Any:
#         if cls not in cls._instances:
#             instance = super().__call__(*args, **kwds)
#             cls._instances[cls] = instance
#         return cls._instances[cls]


# class MetaLogger(type):
#     def __new__(mcs, name, bases, attrs):  # pylint: disable=C0204
#         wrapper_dict = logging.Logger.__dict__.copy()
#         for key, val in wrapper_dict.items():
#             if key not in attrs and key != "__reduce__":
#                 attrs[key] = val
#         return type.__new__(mcs, name, bases, attrs)



__all__ = [ "with_metaclass", "MetaParams", "ParamsBase" ]


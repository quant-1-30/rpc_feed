#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Feb 16 13:56:19 2019

@author: python
"""
import pandas as pd
import contextlib
import functools
import logging
import pdb
import time
import warnings
import sys
import weakref
import threading
import inspect

from functools import wraps
from contextlib import contextmanager


def singleton(cls):

    instances = {}
    @functools.wraps(cls)
    def get_instance(*args, **kw):
        with threading.Lock():
            if cls not in instances:
                instances[cls] = cls(*args, **kw)
            return instances[cls]
    return get_instance


def async_method_warning(sync_method):
    @wraps(sync_method)
    async def async_method(*args, **kwargs):
        warnings.warn(f"{sync_method.__name__} is sync method, please use a{sync_method.__name__} instead.")
        return sync_method(*args, **kwargs)
    return async_method


def _deprecated_getitem_method(name, attrs):
    """Create a deprecated ``__getitem__`` method that tells users to use
    getattr instead.

    Parameters
    ----------
    name : str
        The name of the object in the warning message.
    attrs : iterable[str]
        The set of allowed attributes.

    Returns
    -------
    __getitem__ : callable[any, str]
        The ``__getitem__`` method to put in the class dict.
    """
    attrs = frozenset(attrs)
    msg = (
        "'{name}[{attr!r}]' is deprecated, please use"
        " '{name}.{attr}' instead"
    )

    def __getitem__(self, key):
        """``__getitem__`` is deprecated, please use attribute access instead.
        """
        warnings.warn(msg.format(name=name, attr=key), DeprecationWarning, stacklevel=2)
        if key in attrs:
            return getattr(self, key)
        raise KeyError(key)

    return __getitem__


class Deprecated(object):

    def __init__(self, tip_info=''):
        self.tip_info = tip_info

    def __call__(self, obj):
        if isinstance(obj, type):
            return self._decorate_class(obj)
        else:
            return self._decorate_fun(obj)

    def _decorate_class(self, cls):

        msg = "class {} is deprecated".format(cls.__name__)
        if self.tip_info:
            msg += "; {}".format(self.tip_info)
        init = cls.__init__

        def wrapped(*args, **kwargs):
            warnings.warn(msg, category=DeprecationWarning)
            return init(*args, **kwargs)

        cls.__init__ = wrapped

        wrapped.__name__ = '__init__'
        wrapped.__doc__ = self._update_doc(init.__doc__)
        wrapped.deprecated_original = init
        return cls

    def _decorate_fun(self, fun):

        msg = "func {} is deprecated".format(fun.__name__)
        if self.tip_info:
            msg += "; {}".format(self.tip_info)

        def wrapped(*args, **kwargs):
            warnings.warn(msg, category=DeprecationWarning)
            return fun(*args, **kwargs)

        wrapped.__name__ = fun.__name__
        wrapped.__dict__ = fun.__dict__
        wrapped.__doc__ = self._update_doc(fun.__doc__)
        return wrapped

    def _update_doc(self, func_doc):
        deprecated_doc = "Deprecated"
        if self.tip_info:
            """如果有tip format tip"""
            deprecated_doc = "{}: {}".format(deprecated_doc, self.tip_info)
        if func_doc:
            func_doc = "{}\n{}".format(deprecated_doc, func_doc)
        return func_doc


def warnings_filter(func):

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        warnings.simplefilter('ignore')
        ret = func(*args, **kwargs)
        if not Env.g_ignore_all_warnings:
            warnings.simplefilter('default')
        return ret
    return wrapper


def catch_error(return_val=None, log=True):

    def decorate(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logging.exception(e) if log else logging.debug(e)
                return return_val

        return wrapper

    return decorate


def consume_time(func):

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print('{} cost {}s'.format(func.__name__, round(end_time - start_time, 3)))
        return result

    return wrapper


def empty_wrapper(func):

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper


# noinspection PyUnusedLocal
def empty_wrapper_with_params(*p_args, **p_kwargs):

    def decorate(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        return wrapper
    return decorate


def except_debug(func):

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            pdb.set_trace()
            print(e)
            return func(*args, **kwargs)
    return wrapper


class LazyFunc(object): #  __getattribute__ > __getattr__(AttributeError)

    def __init__(self, func):
        self.func = func
        self.cache = weakref.WeakKeyDictionary()

    def __get__(self, instance, owner):
        if instance is None:
            return self
        try:
            return self.cache[instance]
        except KeyError:
            ret = self.func(instance)
            self.cache[instance] = ret
            return ret

    def __set__(self, instance, value):
        raise AttributeError("LazyFunc set value!!!")

    def __delete__(self, instance):
        del self.cache[instance]


class LazyClsFunc(LazyFunc):

    def __get__(self, instance, owner):
        """描述器__get__, 使用__get__(owner, owner)替换原始__get__(self, instance, owner)"""
        return super(LazyClsFunc, self).__get__(owner, owner)


def valid_check(func):

    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        if self.valid:
            return func(self, *args, **kwargs)
        else:
            logging.info('metric input is invalid or zero order gen!')

    return wrapper


def _validate_type(_type=(list,tuple)):
    def decorate(func):
        def wrap(*args):
            res = func(*args)
            if not isinstance(res, _type):
                raise TypeError('can not algorithm type:%s' % _type)
            return res
        return wrap
    return decorate


def deprecated(msg=None, stacklevel=2): # Used to mark a function as deprecated

    def deprecated_dec(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            warnings.warn(
                msg or "Function %s is deprecated." % fn.__name__,
                category=DeprecationWarning,
                stacklevel=stacklevel
            )
            return fn(*args, **kwargs)
        return wrapper
    return deprecated_dec


def require_not_initialized(exception):
    """
    Decorator for API methods that should only be called during or before
    TradingAlgorithm.initialize.  `exception` will be raised if the method is
    called after initialize.

    Examples
    --------
    @require_not_initialized(SomeException("Don't do that!"))
    def method(self):
        # Do stuff that should only be allowed during initialize.
    """
    def decorator(method):
        @wraps(method)
        def wrapped_method(self, *args, **kwargs):
            if self.initialized: # not self.initialized /self._in_before_trading_start
                raise exception
            return method(self, *args, **kwargs)
        return wrapped_method
    return decorator


def _make_unsupported_method(name):
    def method(*args, **kwargs):
        raise NotImplementedError(
            "Method %s is not supported on LabelArrays." % name
        )
    method.__name__ = name
    method.__doc__ = "Unsupported LabelArray Method: %s" % name
    return method

def remove_na(f):
    @wraps(f)
    def wrapper(*args):
        result = f(*args)
        if isinstance(result, (pd.DataFrame, pd.Series)):
            result.dropna(inplace=True)
        return result
    return wrapper


def coerce_numbers_to_my_dtype(f):
    """
    A decorator for methods whose signature is f(self, other) that coerces
    ``other`` to ``self.dtype``.

    This is used to make comparison operations between numbers and `Factor`
    instances work independently of whether the user supplies a float or
    integer literal.

    For example, if I write::

        my_filter = my_factor > 3

    my_factor probably has dtype float64, but 3 is an int, so we want to coerce
    to float64 before doing the comparison.
    """
    @wraps(f)
    def method(self, other):
        if isinstance(other, Number):
            other = coerce_to_dtype(self.dtype, other)
        return f(self, other)
    return method


def api_method(f): # patch func to instance func
    # Decorator that adds the decorated class method as a callable
    # function (wrapped) to zipline.api
    @wraps(f)
    def wrapped(*args, **kwargs):
        # Get the instance and call the method
        algo_instance = get_algo_instance()
        if algo_instance is None:
            raise RuntimeError(
                'api method %s'
                % f.__name__
            )
        return getattr(algo_instance, f.__name__)(*args, **kwargs)
    return f


class Registry():
        
    _module_dict = dict()

    def get(self, alias):
        return self._module_dict.get(alias, None)

    def _register_module(self, module_class):
        """Register a module.
        Args:
            module (:obj:`nn.Module`): Module to be registered.
        """
        if not inspect.isclass(module_class):
            raise TypeError(
                "module must be a class, but got {}".format(type(module_class))
            )
        # metaclass
        module_name = module_class.__name__.split("_")[-1]
        if module_name in self._module_dict:
            raise KeyError(
                "{} is already registered".format(module_name)
            )
        self._module_dict[module_name] = module_class

    def __call__(self, _obj):
        # pdb.set_trace()
        print("register _obj ", _obj)
        self._register_module(_obj)
        return _obj
    
    # def __repr__(self):
    #     format_str = self.__class__.__name__ + "(name={}, items={})".format(
    #         self._obj.__name__, list(self._module_dict.keys())
    #     )
    #     return format_str

registry = Registry()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import inspect
import os
import pdb
from meta import SingletonMeta


class Registry(metaclass=SingletonMeta):
        
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


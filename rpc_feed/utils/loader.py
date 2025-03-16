#!/usr/bin/env python3
# -*- coding:utf-8 -*-

import os
import re
import sys
import logging
import inspect
import importlib
from colorama import Fore
from types import ModuleType
from typing import Union
from pathlib import Path


def get_all_classes_inherited_MLBase(module_path, base_cls):
    # script_path = os.path.join(current_path, "app", "%s.py" % module_name)
    sys.path.append(os.path.dirname(module_path))
    module_name = os.path.split(module_path)[-1].replace(".py", "")
    try:
        module = importlib.import_module(module_name)
    except ModuleNotFoundError as e:
        print(Fore.RED + 'Can\'t import module "' + module_name + f'", reason: {e}.\n'
                                                                  'If you are looking for examples, you can find a dummy base.py here:\n' +
              Fore.LIGHTYELLOW_EX + 'https://labelstud.io/tutorials/dummy_model.html')
        module = None
        exit(-1)
    # print('module', module)
    class_set = []
    for name, obj in inspect.getmembers(module, inspect.isclass):
        # print('name, obj, bases', name, obj, obj.__base__)
        if name == base_cls.__name__:
            continue
        if issubclass(obj, base_cls):
            # names.add(name)
            class_set.append(obj)
            # return name, obj
    return class_set


def get_module_by_module_path(module_path: Union[str, ModuleType]):
    """Load module path

    :param module_path:
    :return:
    :raises: ModuleNotFoundError
    """
    module_path = os.path.expanduser(module_path)
    print("expanduser path ", module_path)
    if module_path is None:
        raise ModuleNotFoundError("None is passed in as parameters as module_path")

    if isinstance(module_path, ModuleType):
        module = module_path
    else:
        if module_path.endswith(".py"):
            module_name = re.sub("^[^a-zA-Z_]+", "", re.sub("[^0-9a-zA-Z_]", "", module_path[:-3].replace("/", "_")))
            module_spec = importlib.util.spec_from_file_location(module_name, module_path)
            module = importlib.util.module_from_spec(module_spec)
            sys.modules[module_name] = module
            module_spec.loader.exec_module(module)
        else:
            module = importlib.import_module(module_path)
    return module


def _get_possible_module_path(paths):
    ret = []
    for p in paths:
        p = Path(p)
        for path in p.glob("*"):
            if path.suffix in ["py", ".so"] or (path.is_dir()):
                if path.stem.isidentifier():
                    ret.append(path)
    return ret


def _get_regular_import_name(path, module_paths):
    path = Path(path)
    for mp in module_paths:
        mp = Path(mp)
        if mp == path:
            return path.stem
        try:
            relative_path = path.relative_to(Path(mp))
            parts = list((relative_path.parent / relative_path.stem).parts)
            module_name = ".".join([mp.stem] + parts)
            return module_name
        except Exception:
            pass
    return None


def import_file(path, name: str = None, add_to_sys=True, disable_warning=False):
    global CUSTOM_LOADED_MODULES
    path = Path(path)
    module_name = path.stem
    try:
        user_paths = os.environ["PYTHONPATH"].split(os.pathsep)
    except KeyError:
        user_paths = []
    possible_paths = _get_possible_module_path(user_paths)
    model_import_name = _get_regular_import_name(path, possible_paths)
    if model_import_name is not None:
        return import_name(model_import_name)
    if name is not None:
        module_name = name
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if not disable_warning:
        logging.warning(
            (
                f"Failed to perform regular import for file {path}. "
                "this means this file isn't in any folder in PYTHONPATH "
                "or don't have __init__.py in that project. "
                "directly file import may fail and some reflecting features are "
                "disabled even if import succeed. please add your project to PYTHONPATH "
                "or add __init__.py to ensure this file can be regularly imported. "
            )
        )

    if add_to_sys:  # this will enable find objects defined in a file.
        # avoid replace system modules.
        if module_name in sys.modules and module_name not in CUSTOM_LOADED_MODULES:
            raise ValueError(f"{module_name} exists in system.")
        CUSTOM_LOADED_MODULES[module_name] = module
        sys.modules[module_name] = module
    return module


def import_name(name, package=None):
    module = importlib.import_module(name, package)
    return module

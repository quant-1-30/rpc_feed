#! /usr/bin/env python3
# -*- coding: utf-8 -*-

from .provider import Instrument, Tick, Daily, Close, Adjust, Right


_providers = dict(
    (
    ("asset", Instrument()),
    ("tick", Tick()),
    ("daily", Daily()),
    ("close", Close()),
    ("adjust", Adjust()),
    ("right", Right()),
    ))

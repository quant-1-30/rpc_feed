#! /usr/bin/env python3
# -*- coding: utf-8 -*-

from .provider import Instrument, Tick, Close, Adjust, Right


_providers = dict(
    (
    ("asset", Instrument()),
    ("tick", Tick()),
    ("close", Close()),
    ("adjust", Adjust()),
    ("right", Right()),
    ))

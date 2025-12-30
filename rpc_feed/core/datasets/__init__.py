#! /usr/bin/env python3
# -*- coding: utf-8 -*-

from core.datasets.provider import TradingCalendar, Instrument, Index, Tick, Close, Adjust, Right


_providers = dict(
    (
    ("calendar", TradingCalendar()),
    ("asset", Instrument()),
    ("index", Index()),
    ("tick", Tick()),
    ("close", Close()),
    ("adjust", Adjust()),
    ("right", Right()),
    ))

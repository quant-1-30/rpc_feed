#! /user/bin/env python3
# -*- coding: utf-8 -*-

import unittest
import pytest
from sim.quoteApi import QuoteApi


class TestQuote(unittest.TestCase):

    def setUp(self) -> None:
        return super().setUp()

    def test_calendar(self):
        calendars = quoteApi.onSubCalendar()

    def test_asset(self):
        assets = quoteApi.onSubAsset(date=20240326)

    def test_tick(self):
        tick_datas = quoteApi.onSubTickData(date=20211015)

    def test_dataset(self):
        datasets = quoteApi.OnSubDatasets(s_date=20211001, e_date=20211109, sid=['600000'])

    def test_event(self):
        adjustments = quoteApi.onSubEvent(start_date=20000301, end_date=20240401, event_type="adjustment")
        print("adjustments", adjustments)
        rightments = quoteApi.onSubEvent(start_date=20000301, end_date=20240401, event_type="rightment")
        print("rightsments", rightments)

 
    def tearDown(self) -> None:
        return super().tearDown()

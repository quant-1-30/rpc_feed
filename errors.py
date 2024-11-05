#!/usr/bin/env python3
# -*- coding: utf-8 -*-


class NoDataOnDate(Exception):
    """
    Raised when a spot price cannot be found for the sid and date.
    """
    pass


class NoDataBeforeDate(NoDataOnDate):
    pass


class NoDataAfterDate(NoDataOnDate):
    pass


class NoDataForSid(Exception):
    """
    Raised when the requested sid is missing from the pricing data.
    """
    pass

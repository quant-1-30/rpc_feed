#！/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
from toolz import valmap
from metabase import ParamBase


class ExLib(ParamBase):

    params = (
        ("alias", "ex_right"),
    )
    
    @staticmethod
    def _calculate_dividends(adjustment, kline):
        """
           股权登记日后的下一个交易日就是除权日或除息日，这一天购入该公司股票的股东不再享有公司此次分红配股
           前复权：复权后价格=(复权前价格-现金红利)/(1+流通股份变动比例)
           后复权：复权后价格=复权前价格×(1+流通股份变动比例)+现金红利
        """
        try:
            dividends = adjustment['dividends']
            # print('dividends union', set(dividends.index) & set(kline.index))
            ex_close = kline['close'].reindex(index=dividends.index)
            qfq = (1 - dividends['bonus']/(10 * ex_close)) / \
                  (1 + (dividends['sid_bonus'] + dividends['sid_transfer']) / 10)
        except KeyError:
            qfq = pd.Series(dtype=float)
        return qfq

    @staticmethod
    def _calculate_rights_for_sid(adjustment, kline):
        """
           配股除权价=（除权登记日收盘价+配股价*每股配股比例）/(1+每股配股比例）
        """
        try:
            rights = adjustment['rights']
            # print('rights', sid, rights)
            ex_close = kline['close'].reindex(index=rights.index)
            # print('ex_close', ex_close)
            qfq = (ex_close + (rights['rights_price'] * rights['rights_bonus']) / 10) / \
                  (1 + rights['rights_bonus']/10)
        except KeyError:
            qfq = pd.Series(dtype=float)
        return qfq

    def _calculate_fq(self, adjustments, rights, kline):
        # adjustments['dividends'] = valmap(lambda x: reformat(x), adjustments['dividends'])
        fq_dividends = self._calculate_dividends(adjustments, kline)
        fq_rights = self._calculate_rights(rights, kline)
        fq = fq_dividends.append(fq_rights)
        fq.sort_index(ascending=False, inplace=True)
        qfq = 1 / fq.cumprod()
        # print('qfq', qfq)
        return qfq

    def on_handle(self, adjs, rgts, lines):
        # raise NotImplementedError("import from libs pyx")
        fq = self._calculate_fq(adjs, rgts, lines)
        return fq 

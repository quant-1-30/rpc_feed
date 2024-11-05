# Copyright 2016 Quantopian, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import numpy as np
import pandas as pd
from functools import wraps, partial, reduce
from typing import Iterator, Union, Dict
from meta import ParamBase
from utils.utility import no_hup


class Aggregator(ParamBase):
    """
    Converts minute pricing data into a daily summary, to be used for the
    last slot in a call to history with a frequency of `1d`.

    This summary is the same as a daily bar rollup of minute data, with the
    distinction that the summary is truncated to the `dt` requested.
    i.e. the aggregation slides forward during a the course of simulation day.

    Provides aggregation for `open`, `high`, `low`, `close`, and `volume`.
    The aggregation rules for each price type is documented in their respective

    """
    params = {
        # ("alias", "aggregator"),
        ("tick", "minute"),
    }
    # The int value is used for deltas to avoid extra computation from
    # creating new Timestamps.
    # self._one_min = pd.Timedelta('1 min').value
    # window[~pd.isnull(window)] / tz="UTC" / pd.isnull / np.nan

    def _opens(self, datasets, freq):
        """
        The open field's aggregation returns the first value that occurs
        for the day, if there has been no data on or before the `dt` the open
        is `nan`.

        Once the first non-nan open is seen, that value remains constant per
        asset for the remainder of the day.

        Returns
        -------
        np.array with dtype=float64, in order of assets parameter.
        """
        opens = np.array(datasets.pop("open"))
        res = opens[0]
        return res

    def _highs(self, datasets, freq):
        """
        The high field's aggregation returns the largest high seen between
        the market open and the current dt.
        If there has been no data on or before the `dt` the high is `nan`.

        Returns
        -------
        np.array with dtype=float64, in order of assets parameter.
        """
        highs = np.array(datasets.pop("high"))
        res = np.max(highs)
        return res

    def _lows(self, datasets, freq):
        """
        The low field's aggregation returns the smallest low seen between
        the market open and the current dt.
        If there has been no data on or before the `dt` the low is `nan`.

        Returns
        -------
        np.array with dtype=float64, in order of assets parameter.
        """
        lows = np.array(datasets.pop("low"))
        res = np.min(lows)
        return res

    def closes(self, datasets):
        """
        The close field's aggregation returns the latest close at the given
        dt.
        If the close for the given dt is `nan`, the most recent non-nan
        `close` is used.
        If there has been no data on or before the `dt` the close is `nan`.

        Returns
        -------
        np.array with dtype=float64, in order of assets parameter.
        """
        # window[~np.isnan(window)][-1]
        closes = np.array(datasets.pop("close"))
        res = closes[-1]
        return res

    def _volumes(self, datasets, freq):
        """
        The volume field's aggregation returns the sum of all volumes
        between the market open and the `dt`
        If there has been no data on or before the `dt` the volume is 0.

        Returns
        -------
        np.array with dtype=int64, in order of assets parameter.
        """
        # np.zeros(shape, dtype=np.uint32) / np.full(shape, np.nan)
        volumes = np.array(datasets.pop("volume"))
        res = np.sum(volumes)
        return res

    def _amounts(self, datasets, freq):
        """
        The amounts field's aggregation returns the sum of all volumes
        between the market open and the `dt`
        If there has been no data on or before the `dt` the amount is 0.

        Returns
        -------
        np.array with dtype=int64, in order of assets parameter.
        """
        amounts = np.array(datasets.pop("amount"))
        res = np.sum(amounts)
        return res

    def _prelude(self, data: pd.DataFrame, freq: Union[int, str]):
        if isinstance(data, pd.MultiIndex):
           new_index = data.index.droplevel("sid")
           data.index = new_index
        data.sort_index(ascending=True, inplace=True)
       
        grp_by_mins = data.resample("f{freq}min", origin="start")
        # df.groupby(by=["a"]).groups
        #  idx = bisect.bisect_left()
        res = {
           "opens": grp_by_mins.apply(self.opens),
           "highs": grp_by_mins.apply(self.highs),
           "lows": grp_by_mins.apply(self.lows),
           "closes": grp_by_mins.apply(self.close),
           "volumes": grp_by_mins.apply(self.volumes),
           "amounts": grp_by_mins.apply(self.amounts)
        }
        return res
    
    def on_handle(self, raw_data, freq):
        _par = partial(self._prelude, freq=freq)
        aggregate = dict()
        for instrument, lines in raw_data.items():
            aggregate[instrument] = _par(lines)
        return aggregate


class Grouper(ParamBase):
    """
        groupy by level
    """
    params = (
        # ("alias", "grouper"),
        ("multi_index", ("tick", "sid"))
    )

    @staticmethod
    def _expr(data: pd.DataFrame, level):
        if level == "tick":
            grps = data.groupby(level=level).apply(lambda f: f.T.to_dict)
        else:
            grps = data.groupby(level=level).apply(lambda f: no_hup(f))
        return grps

    def on_handle(self, arrays, level="ticker", timestamp=True):
        """
            by: List[str] or str
        """
        if isinstance(arrays, Iterator):
            lines = np.array()
            sids = []
            for tup, line in arrays:
                lines = np.stack((lines, line))
                sids.append(tup)
        else:
            lines = [array[1] for array in arrays]
            sids = [array[0] for array in arrays]
        
        dataframe = pd.DataFrame(lines)
        if not timestamp:
            _sids = [pd.Timestamp(item[0]) for item in sids]
        multi_index = pd.MultiIndex.from_arrays(_sids, names=self.p.multi_index)
        dataframe.index = multi_index
        # group by
        output = self._expr(data=dataframe, level=level)
        return output

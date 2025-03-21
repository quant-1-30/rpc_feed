#! /usr/bin/env python3 
# -*- coding: utf-8 -*-

from meta import MetaParams, with_metaclass


class MetaProvider(MetaParams):
    """Client Provider

    Requesting data from server as a client. Can propose requests:

        - Calendar : Directly respond a list of calendars
        - Instruments (without filter): Directly respond a list/dict of instruments
        - Instruments (with filters):  Respond a list/dict of instruments
        - Features : Respond a cache uri

    The general workflow is described as follows:
    When the user use client provider to propose a request, the client provider will connect the server and send the request. The client will start to wait for the response. The response will be made instantly indicating whether the cache is available. The waiting procedure will terminate only when the client get the response saying `feature_available` is true.
    `BUG` : Everytime we make request for certain data we need to connect to the server, wait for the response and disconnect from it. We can't make a sequence of requests within one connection. You can refer to https://python-socketio.readthedocs.io/en/latest/client.html for documentation of python-socketIO client.
    
        Local provider class
    It is a set of interface that allow users to access data.
    Because PITD is not exposed publicly to users, so it is not included in the interface.

    To keep compatible with old qlib provider.
    """

    (CONNECTED, DISCONNECTED, CONNBROKEN, DELAYED,
     LIVE, NOTSUBSCRIBED, NOTSUPPORTED_TF, UNKNOWN) = range(8)

    def donew(cls, *args, **kwargs):
        _obj, args, kwargs = super(MetaProvider, cls).donew(*args, **kwargs)
        
        if not hasattr(_obj, "load"):
            raise NotImplementedError("implement load method")
        return _obj, args, kwargs


class Provider(with_metaclass(MetaProvider, object)):
    """Provider

    Provide data from server.
    """
    
    params = (
        ("options", {"chunk_size": 1000, "timeout": 60}),
        ("stream", False),
    )
    
    def load(self, *args, **kwargs):
        pass

    def start(self):
        self._status = self.CONNECTED

    def stop(self):
        self._status = self.DISCONNECTED

    # def clone(self, **kwargs):
    #     return DataClone(dataname=self, **kwargs)

#!/usr/bin/env python
# coding:utf-8

from pypegasus.pgclient import Pegasus
from pypegasus.utils.tools import ScanOptions

from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, Deferred


@inlineCallbacks
def basic_test():
    # init
    c = Pegasus(['10.38.166.18:31601', '10.38.162.231:31601'], 'jiashuo')

    suc = yield c.init()
    if not suc:
        reactor.stop()
        print('ERROR: connect pegasus server failed')
        return

    # set
    try:
        ret = yield c.set('hkey1', 'skey1', 'value', 0, 500)
        print('set ret: ', ret)
    except Exception as e:
        print(e)

    # multi_set
    kvs = {'skey1': 'value1', 'skey2': 'value2', 'skey3': 'value3'}
    ret = yield c.multi_set('hkey3', kvs, 999)
    print('multi_set ret: ', ret)

    # multi_get
    ks = set(kvs.keys())
    ret = yield c.multi_get('hkey3', ks)
    print('multi_get ret: ', ret)

    ret = yield c.multi_get('hkey3', ks, 1)
    print('multi_get ret: ', ret)
    while ret[0] == 7:              # has more data
        ks.remove(ret[1].keys()[0])
        ret = yield c.multi_get('hkey3', ks, 1)
        print('multi_get ret: ', ret)

    ret = yield c.multi_get('hkey3', ks, 100, 10000, True)
    print('multi_get ret: ', ret)

    reactor.stop()


if __name__ == "__main__":
    reactor.callWhenRunning(basic_test)
    reactor.run()

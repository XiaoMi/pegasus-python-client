# coding=utf-8

import time
import struct
from pypegasus.base.ttypes import (
    rocksdb_error_types,
    error_types
)

epoch_begin = 1451606400            # seconds since 2016.01.01-00:00:00 GMT


def dsn_gpid_to_thread_hash(app_id, partition_index):
    return app_id * 7919 + partition_index


def epoch_now():
    return int(time.time()) - epoch_begin


def get_ttl(ttl):
    return 0 if ttl == 0 else epoch_now() + ttl


def convert_error_type(rdb_err):
    if rdb_err == rocksdb_error_types.kNotFound.value:
        return error_types.ERR_OBJECT_NOT_FOUND.value
    elif rdb_err == rocksdb_error_types.kIncomplete.value:
        return error_types.ERR_OBJECT_NOT_FOUND.ERR_INCOMPLETE_DATA.value
    elif rdb_err == rocksdb_error_types.kOk.value:
        return error_types.ERR_OBJECT_NOT_FOUND.ERR_OK.value
    else:
        return rdb_err


class ScanOptions(object):
    """
    configurable options for scan.
    """

    def __init__(self):
        self.timeout_millis = 5000
        self.batch_size = 1000
        self.start_inclusive = True
        self.stop_inclusive = False
        self.snapshot = None                   # for future use

    def __repr__(self):
        lst = ['%s=%r' % (key, value)
               for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(lst))


def restore_key(merge_key):
    s = struct.Struct('>H')
    hash_key_len = s.unpack(merge_key[:2])[0]

    hash_key = merge_key[2:2+hash_key_len]
    sort_key = merge_key[2+hash_key_len:]

    return hash_key, sort_key


def bytes_cmp(left, right):
    min_len = min(len(left), len(right))
    for i in range(min_len):
        r = ord(left[i]) - ord(right[i])
        if r != 0:
            return r

    return len(left) - len(right)

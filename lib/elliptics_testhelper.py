# -*- coding: utf-8 -*-
#
import elliptics
import random
import pytest
import os

import utils

# Позиция offset'а при записи
OffsetWriteGetter = {'BEGINNING':     lambda l: 0,
                     'MIDDLE':        lambda l: random.randint(1, l - 2),
                     'END':           lambda l: random.randint(1, l - 1),
                     'APPENDING':     lambda l: l,
                     'OVER_BOUNDARY': lambda l: random.randint(l + 1, utils.MAX_LENGTH + 2)}

# Значение size'а при чтении
SizeGetter = {'NULL':      lambda l, os=None: 0,
              'DATA_SIZE': lambda l, os=None: l,
              'PART':      lambda l, os=None: random.randint(1, l - 1),
              'OVER_SIZE': lambda l, os=None: random.randint(l + 1, utils.MAX_LENGTH + 2),
              'PART_DEPEND_ON_OFFSET_VALID':   lambda l, os=None: random.randint(1, l - os),
              'PART_DEPEND_ON_OFFSET_INVALID': lambda l, os=None: random.randint(l - os + 1, l)}

# Offset для чтения
OffsetReadGetter = {'NULL':          lambda l: 0,
                    'MIDDLE':        lambda l: random.randint(1, l - 1),
                    'OVER_BOUNDARY': lambda l: random.randint(l + 1, utils.MAX_LENGTH + 2)}

# Значение chunk_size'а при записи
ChunkSizeGetter = {'NULL':      lambda l: 0,
                   'MIDDLE':    lambda l: random.randint(1, l - 1),
                   'DATA_SIZE': lambda l: l,
                   'OVER_SIZE': lambda l: random.randint(l + 1, utils.MAX_LENGTH + 2)}

# Значение длины данных, при записи по offset'у
OffsetDataGetter = {'BEGINNING':     lambda l, os: random.randint(1, l - os - 1),
                    'MIDDLE':        lambda l, os: random.randint(1, l - os - 1),
                    'END':           lambda l, os: l - os,
                    'APPENDING':     lambda l, os: random.randint(l - os + 1, utils.MAX_LENGTH + 1),
                    'OVER_BOUNDARY': lambda l, os: random.randint(1, utils.MAX_LENGTH)}

@pytest.fixture(scope='function')
def key_and_data():
    """ Returns key and data (random sequence of bytes)
    """
    data = utils.get_data()
    key = utils.get_sha1(data)
    return (key, data)

@pytest.fixture(scope='function')
def timestamp():
    """ Returns timestamp
    """
    timestamp = elliptics.Time.now()
    return timestamp

@pytest.fixture(scope='function')
def user_flags():
    """ Returns randomly generated user_flags
    """
    user_flags = random.randint(0, utils.USER_FLAGS_MAX)
    return user_flags

class EllipticsTestHelper(elliptics.Session):
    """ This class extend elliptics.Session class with some useful (for tests) features
    """
    error_info = type("Errors", (), {
            'WrongArguments': "Argument list too long",
            'NotExists': "No such file or directory",
            'TimeoutError': "Connection timed out",
            'AddrNotExists': "No such device or address"
            })

    _log_path = "/var/log/elliptics/client.log"
    
    def __init__(self, nodes, wait_timeout, check_timeout, groups=None, config=elliptics.Config(), logging_level=0):
        if logging_level:
            os.makedirs(os.path.dirname(self._log_path))
            elog = elliptics.Logger(self._log_path, logging_level)
        else:
            elog = elliptics.Logger("/dev/stderr", logging_level)
        client_node = elliptics.Node(elog, config)
        client_node.set_timeouts(wait_timeout, check_timeout)
        for node in nodes:
            client_node.add_remote(node.host, node.port)

        elliptics.Session.__init__(self, client_node)

        if groups is None:
            groups = set()
            for n in nodes:
                groups.add(n.group)
            groups = list(groups)

        self.groups = groups

    @staticmethod
    def wait_for(async_result):
        return async_result.get().pop()

    # Elliptics commands, which return the result of operation
    # (instead of elliptics.AsyncResult)
    def write_data_now(self, key, data, offset=0, chunk_size=0):
        return self.wait_for(self.write_data(key, data, offset=offset, chunk_size=chunk_size))

    def read_data_now(self, key, offset=0, size=0):
        return self.wait_for(self.read_data(key, offset=offset, size=size))
    
    def write_prepare_now(self, key, data, offset, psize):
        return self.wait_for(self.write_prepare(key, data, offset, psize))

    def write_plain_now(self, key, data, offset):
        return self.wait_for(self.write_plain(key, data, offset))

    def write_commit_now(self, key, data, offset, csize):
        return self.wait_for(self.write_commit(key, data, offset, csize))

    # Методы обеспечивающие проверку корректности отработки команд elliptics'а
    def checking_inaccessibility(self, key, data_len=None):
        """ Проверяет верно ли, что данные не доступны по ключу
        """
        try:
            result_data = str(self._session.read_data(key).data)
        except Exception as e:
            print e.message
        else:
            print len(result_data), '/', data_len, 'bytes already accessible'
            assert utils.get_sha1(result_data) != key

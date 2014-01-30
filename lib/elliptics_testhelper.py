# -*- coding: utf-8 -*-
#
import elliptics
import random
import pytest

import utils
from os import urandom, environ

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
    data = utils.get_data()
    key = utils.get_sha1(data)
    return (key, data)

@pytest.fixture(scope='function')
def timestamp():
    timestamp = elliptics.Time.now()
    return timestamp

@pytest.fixture(scope='function')
def user_flags():
    user_flags = random.randint(0, utils.USER_FLAGS_MAX)
    return user_flags

class EllipticsTest:
    """ Класс обеспечивает работу с elliptics'ом на уровне базовых операций.
    """
    errors = type("Errors", (), {
            "WrongArguments": "Argument list too long",
            "NotExists": "No such file or directory"
            })
    
    def __init__(self, hosts, write_timeout, wait_timeout, groups=(1,), config=elliptics.Config()):
        # создаем сессию elliptics'а
        elog = elliptics.Logger("/dev/stderr", 0)
        node = elliptics.Node(elog, config)
        node.set_timeouts(write_timeout, wait_timeout)
        for host in hosts:
            node.add_remote(host, 1025)

        self.es = elliptics.Session(node)
        self.es.groups = groups
        # запоминаем timestamp для последующих проверок
        self.timestamp = elliptics.Time.now()

    # Базовые команды elliptics'а
    def write_data(self, key, data, offset=0, chunk_size=0):
        result = self.es.write_data(key, data, offset=offset, chunk_size=chunk_size)
        result = result.get().pop()
        return result

    def read_data(self, key, offset=0, size=0):
        result = self.es.read_data(key, offset=offset, size=size)
        result = result.get().pop()
        return result
    
    def write_prepare(self, key, data, offset, psize):
        result = self.es.write_prepare(key, data, offset, psize).get().pop()
        return result

    def write_plain(self, key, data, offset):
        result = self.es.write_plain(key, data, offset).get().pop()
        return result

    def write_commit(self, key, data, offset, csize):
        result = self.es.write_commit(key, data, offset, csize).get().pop()
        return result

    # Методы обеспечивающие проверку корректности отработки команд elliptics'а
    def checking_inaccessibility(self, key, data_len=None):
        """ Проверяет верно ли, что данные не доступны по ключу
        """
        try:
            result_data = str(self.es.read_data(key).data)
        except Exception as e:
            print e.message
        else:
            print len(result_data), '/', data_len, 'bytes already accessible'
            assert utils.get_sha1(result_data) != key

    def set_user_flags(self, user_flags):
        """ Устанавливает user_flags атрибут в случайное значение
        для команд записи
        """
        self.es.user_flags = user_flags

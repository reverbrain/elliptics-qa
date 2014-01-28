# -*- coding: utf-8 -*-
#
import pytest
import random
import elliptics

from hamcrest import assert_that, equal_to, calling, raises, has_property, has_length, is_

import elliptics_testhelper as et
from elliptics_testhelper import key_and_data, timestamp, user_flags
import utils
from utils import elliptics_result_with

config = pytest.config

WRITE_TIMEOUT = config.getoption("write_timeout")
WAIT_TIMEOUT = config.getoption("wait_timeout")

HOSTS = config.getoption("host")

elliptics_test = et.EllipticsTest(HOSTS, WRITE_TIMEOUT, WAIT_TIMEOUT)

def get_length(data):
    return sum(len(i) for i in data)

@pytest.mark.pwctest
def test_prepare_write_commit(timestamp, user_flags):
    """ Тест записи по схеме prepare-write-commit
    """
    key, data = utils.get_key_and_data_list()
    data_len = get_length(data)
    elliptics_test.set_user_flags(user_flags)

    elliptics_test.write_prepare(key, data[0], 0, data_len)
    elliptics_test.write_plain(key, data[1], len(data[0]))
    elliptics_test.write_commit(key, data[2], len(data[0]) + len(data[1]), data_len)

    result = elliptics_test.read_data(key)
    data = ''.join(data)

    assert_that(result, is_(elliptics_result_with(error_code=0,
                                                  timestamp=timestamp,
                                                  user_flags=user_flags,
                                                  data=data)))

@pytest.mark.pwctest
def test_pwc_inaccessibility(timestamp, user_flags):
    """ Тест недоступности данных после записи по
    схеме prepare-write-commit
    """
    key, data = utils.get_key_and_data_list()
    data_len = get_length(data)
    elliptics_test.set_user_flags(user_flags)

    elliptics_test.write_prepare(key, data[0], 0, data_len)
    elliptics_test.checking_inaccessibility(key, data_len)
    elliptics_test.write_plain(key, data[1], len(data[0]))
    elliptics_test.checking_inaccessibility(key, data_len)
    elliptics_test.write_commit(key, data[2], len(data[0]) + len(data[1]), data_len)

    result = elliptics_test.read_data(key)
    data = ''.join(data)

    assert_that(result, is_(elliptics_result_with(error_code=0,
                                                  timestamp=timestamp,
                                                  user_flags=user_flags,
                                                  data=data)))

@pytest.mark.pwctest
def test_prepare_write_write_commit(timestamp, user_flags):
    """ Тест записи по схеме pwc общего случая
    (несколько команд write_plain)
    """
    key, data = utils.get_key_and_data_list(list_size=random.randint(4, 10))
    data_len = get_length(data)
    elliptics_test.set_user_flags(user_flags)

    elliptics_test.write_prepare(key, data[0], 0, data_len)
    for p in xrange(1, len(data) - 1):
        elliptics_test.write_plain(key, data[p], get_length(data[:p]))
    elliptics_test.write_commit(key, data[-1], data_len - len(data[-1]), data_len)

    result = elliptics_test.read_data(key)
    data = ''.join(data)
    
    assert_that(result, is_(elliptics_result_with(error_code=0,
                                                  timestamp=timestamp,
                                                  user_flags=user_flags,
                                                  data=data)))

@pytest.mark.pwctest
def test_prepare_commit(timestamp, user_flags):
    """ Тест отработки write_prepare-write_commit
    """
    key, data = utils.get_key_and_data_list(list_size=2)
    data_len = get_length(data)
    elliptics_test.set_user_flags(user_flags)

    elliptics_test.write_prepare(key, data[0], 0, data_len)
    elliptics_test.write_commit(key, data[1], len(data[0]), data_len)

    result = elliptics_test.read_data(key)
    data = ''.join(data)
    
    assert_that(result, is_(elliptics_result_with(error_code=0,
                                                  timestamp=timestamp,
                                                  user_flags=user_flags,
                                                  data=data)))

@pytest.mark.pwctest
def test_commit(key_and_data):
    """ Тест выполнения команды write_commit по ключу без подготовительных действий
    """
    key, data = key_and_data

    assert_that(calling(elliptics_test.write_commit).with_args(key, data, 0, len(data)),
                raises(Exception, elliptics_test.errors.NotExists))

@pytest.mark.pwctest
def test_pwc_not_entire_data(timestamp, user_flags):
    """ Тест записи по схеме pwc psize и csize > data size
    """
    key, data = utils.get_key_and_data_list()
    additional_length = random.randint(1, utils.MAX_LENGTH >> 1)
    data_len = get_length(data) + additional_length
    elliptics_test.set_user_flags(user_flags)

    elliptics_test.write_prepare(key, data[0], 0, data_len)
    elliptics_test.write_plain(key, data[1], len(data[0]))
    elliptics_test.write_commit(key, data[2], len(data[0]) + len(data[1]), data_len)

    result = elliptics_test.read_data(key)
    
    assert_that(result, has_property('data', has_length(data_len)))

    data = ''.join(data) + str(result.data)[data_len-additional_length:]

    assert_that(result, is_(elliptics_result_with(error_code=0,
                                                  timestamp=timestamp,
                                                  user_flags=user_flags,
                                                  data=data)))

@pytest.mark.pwctest
def test_pwc_more_than_prepared(key_and_data):
    """ Тест записи по схеме pwc, при попытке записать больше данных
    чем заготовливаем командой write_prepare (data size > psize)
    """
    key, data = key_and_data
    data_len = len(data) - 1

    assert_that(calling(elliptics_test.write_prepare).with_args(key, data[0], 0, data_len),
                raises(Exception, elliptics_test.errors.WrongArguments))

@pytest.mark.pwctest
def test_pwc_prepare_with_offset(timestamp, user_flags):
    """ Тест записи по схеме pwc с начальным offset != 0
    """
    key, data = utils.get_key_and_data_list()
    data_len = get_length(data)
    elliptics_test.set_user_flags(user_flags)

    offset = random.randint(1, utils.MAX_LENGTH)

    elliptics_test.write_prepare(key, data[0], offset, data_len)
    elliptics_test.write_plain(key, data[1], offset + len(data[0]))
    elliptics_test.write_commit(key, data[2], offset + len(data[0]) + len(data[1]), data_len)

    result = elliptics_test.read_data(key)
    
    assert_that(result, has_property('data', has_length(offset + data_len)))

    data = str(result.data)[:offset] + ''.join(data)

    assert_that(result, is_(elliptics_result_with(error_code=0,
                                                  timestamp=timestamp,
                                                  user_flags=user_flags,
                                                  data=data)))

@pytest.mark.pwctest
def test_prepare_writedata():
    """ Тест отработки последовательного выполнения команд
    write_prepare-write_data
    """
    key, data = utils.get_key_and_data_list(list_size=2)
    data_len = get_length(data)

    elliptics_test.write_prepare(key, data[0], 0, data_len)

    assert_that(calling(elliptics_test.write_data).with_args(key, data[1]),
                raises(Exception))

@pytest.mark.pwctest
def test_prepare_write_writedata():
    """ Тест отработки последовательного выполнения команд
    write_prepare-write_plain-write_data
    """
    key, data = utils.get_key_and_data_list()
    data_len = get_length(data)

    elliptics_test.write_prepare(key, data[0], 0, data_len)
    elliptics_test.write_plain(key, data[1], len(data[0]))

    assert_that(calling(elliptics_test.write_data).with_args(key, data[2]),
                raises(Exception))

@pytest.mark.pwctest
def test_pwc_psize_more_than_csize(timestamp, user_flags):
    """ Тест записи данных при psize > csize (psize > data_len)
    """
    key, data = utils.get_key_and_data_list()
    data_len = get_length(data)
    elliptics_test.set_user_flags(user_flags)

    add_len = random.randint(1, utils.MAX_LENGTH >> 1)
    elliptics_test.write_prepare(key, data[0], 0, data_len + add_len)
    elliptics_test.write_plain(key, data[1], len(data[0]))
    elliptics_test.write_commit(key, data[2], len(data[0]) + len(data[1]), data_len)

    result = elliptics_test.read_data(key)
    
    data = ''.join(data)

    assert_that(result, is_(elliptics_result_with(error_code=0,
                                                  timestamp=timestamp,
                                                  user_flags=user_flags,
                                                  data=data)))

@pytest.mark.pwctest
def test_pwc_psize_more_than_csize_negative():
    """ Тест записи данных при psize > csize (csize < data_len)
    """
    key, data = utils.get_key_and_data_list()
    data_len = get_length(data)

    sub_len = random.randint(1, data_len - 1)
    elliptics_test.write_prepare(key, data[0], 0, data_len)
    elliptics_test.write_plain(key, data[1], len(data[0]))

    assert_that(calling(elliptics_test.write_commit).with_args(key, data[2], len(data[0]) + len(data[1]), data_len - sub_len),
                raises(Exception))

@pytest.mark.pwctest
def test_pwc_psize_less_than_csize_negative():
    """ Тест записи данных при psize < csize (psize < data_len)
    """
    key, data = utils.get_key_and_data_list()
    data_len = get_length(data)

    sub_len = random.randint(1, len(data[1]) + len(data[2]) - 1)
    elliptics_test.write_prepare(key, data[0], 0, data_len - sub_len)

    assert_that(calling(elliptics_test.write_plain).with_args(key, data[1], len(data[0])),
                raises(Exception))

@pytest.mark.pwctest
def test_pwc_psize_less_than_csize(timestamp, user_flags):
    """ Тест записи данных при psize < csize (csize > data_len)
    """
    key, data = utils.get_key_and_data_list()
    data_len = get_length(data)
    elliptics_test.set_user_flags(user_flags)

    add_len = random.randint(1, utils.MAX_LENGTH)
    elliptics_test.write_prepare(key, data[0], 0, data_len)
    elliptics_test.write_plain(key, data[1], len(data[0]))
    elliptics_test.write_commit(key, data[2], len(data[0]) + len(data[1]), data_len + add_len)

    result = elliptics_test.read_data(key)
    
    assert_that(result, has_property('data', has_length(data_len + add_len)))

    data = ''.join(data) + str(result.data)[data_len:]

    assert_that(result, is_(elliptics_result_with(error_code=0,
                                                  timestamp=timestamp,
                                                  user_flags=user_flags,
                                                  data=data)))

@pytest.mark.pwctest
def test_pwc_null_psize(timestamp, user_flags):
    """ Тест записи данных при psize == 0
    """
    key, data = utils.get_key_and_data_list()
    data_len = get_length(data)
    elliptics_test.set_user_flags(user_flags)

    elliptics_test.write_prepare(key, data[0], 0, 0)
    elliptics_test.write_plain(key, data[1], len(data[0]))
    elliptics_test.write_commit(key, data[2], len(data[0]) + len(data[1]), data_len)

    result = elliptics_test.read_data(key)
    
    data = ''.join(data)

    assert_that(result, is_(elliptics_result_with(error_code=0,
                                                  timestamp=timestamp,
                                                  user_flags=user_flags,
                                                  data=data)))

@pytest.mark.pwctest
def test_pwc_null_csize(timestamp, user_flags):
    """ Тест записи данных при csize == 0
    """
    key, data = utils.get_key_and_data_list()
    data_len = get_length(data)
    elliptics_test.set_user_flags(user_flags)

    elliptics_test.write_prepare(key, data[0], 0, data_len)
    elliptics_test.write_plain(key, data[1], len(data[0]))
    elliptics_test.write_commit(key, data[2], len(data[0]) + len(data[1]), 0)

    result = elliptics_test.read_data(key)
    
    data = ''

    assert_that(result, is_(elliptics_result_with(error_code=0,
                                                  timestamp=timestamp,
                                                  user_flags=user_flags,
                                                  data=data)))

@pytest.mark.pwctest
def test_pwc_prepare_less_than_data1(key_and_data):
    """ Тест записи данных при попытке записи больше данных чем psize
    при выполнении write_prepare (data1 size > psize)
    """
    key, data = key_and_data

    assert_that(calling(elliptics_test.write_prepare).with_args(key, data, 0, len(data) - 1),
                raises(Exception, elliptics_test.errors.WrongArguments))

# -*- coding: utf-8 -*-
#
import pytest
import elliptics

from hamcrest import assert_that, equal_to, calling, raises, is_

import elliptics_testhelper as et
from elliptics_testhelper import key_and_data, timestamp, user_flags
import utils
from utils import elliptics_result_with

from testcases import size_type_list, offset_type_positive_list, offset_type_negative_list, \
    offset_and_size_types_positive_list, offset_and_size_types_negative_list

config = pytest.config

WRITE_TIMEOUT = config.getoption("write_timeout")
WAIT_TIMEOUT = config.getoption("wait_timeout")

HOSTS = config.getoption("host")

elliptics_test = et.EllipticsTest(HOSTS, WRITE_TIMEOUT, WAIT_TIMEOUT)

@pytest.mark.readtest
def test_write_read(key_and_data, timestamp, user_flags):
    """ Тест чтения-записи данных целиком
    """
    key, data = key_and_data
    elliptics_test.set_user_flags(user_flags)
    
    elliptics_test.write_data(key, data)
    result = elliptics_test.read_data(key)

    assert_that(result, is_(elliptics_result_with(error_code=0,
                                                  timestamp=timestamp,
                                                  user_flags=user_flags,
                                                  data=data)))

@pytest.mark.readtest
@pytest.mark.parametrize("size_type", size_type_list)
def test_read_with_size(size_type, key_and_data, timestamp, user_flags):
    """ Тест чтения с параметром size
    """
    key, data = key_and_data
    size = et.SizeGetter[size_type](len(data))
    elliptics_test.set_user_flags(user_flags)

    elliptics_test.write_data(key, data)
    result = elliptics_test.read_data(key, size=size)

    if not size:
        size = len(data)
    data = data[:size]

    assert_that(result, is_(elliptics_result_with(error_code=0,
                                                  timestamp=timestamp,
                                                  user_flags=user_flags,
                                                  data=data)))
    
@pytest.mark.readtest
@pytest.mark.parametrize("offset_type", offset_type_positive_list)
def test_read_with_offset_positive(offset_type, key_and_data, timestamp, user_flags):
    """ Тест чтения с параметром offset на positive тест-кейсах
    """
    key, data = key_and_data
    offset = et.OffsetReadGetter[offset_type](len(data))
    elliptics_test.set_user_flags(user_flags)
    # подготавливаем данные для чтения
    elliptics_test.write_data(key, data)
    
    result = elliptics_test.read_data(key, offset=offset)

    data = data[offset:]

    assert_that(result, is_(elliptics_result_with(error_code=0,
                                                  timestamp=timestamp,
                                                  user_flags=user_flags,
                                                  data=data)))

@pytest.mark.readtest
@pytest.mark.parametrize("offset_type", offset_type_negative_list)
def test_read_with_offset_negative(offset_type, key_and_data):
    """ Тест чтения с параметром offset на negative тест-кейсах
    """
    key, data = key_and_data
    offset = et.OffsetReadGetter[offset_type](len(data))
    # подготавливаем данные для чтения
    elliptics_test.write_data(key, data)

    assert_that(calling(elliptics_test.read_data).with_args(key, offset=offset),
                raises(Exception, elliptics_test.errors.WrongArguments))

@pytest.mark.readtest
@pytest.mark.parametrize(("offset_type", "size_type"), offset_and_size_types_positive_list)
def test_read_with_offset_and_size_positive(offset_type, size_type,
                                            key_and_data, timestamp, user_flags):
    """ Тест чтения с параметрами offset и size на positive тест-кейсах
    """
    key, data = key_and_data
    offset = et.OffsetReadGetter[offset_type](len(data))
    size = et.SizeGetter[size_type](len(data), offset)
    elliptics_test.set_user_flags(user_flags)
    # подготавливаем данные для чтения
    elliptics_test.write_data(key, data)
    result = elliptics_test.read_data(key, offset=offset, size=size)

    if not size:
        size = len(data)
    data = data[offset:offset+size]

    assert_that(result, is_(elliptics_result_with(error_code=0,
                                                  timestamp=timestamp,
                                                  user_flags=user_flags,
                                                  data=data)))

@pytest.mark.readtest
@pytest.mark.parametrize(("offset_type", "size_type"), offset_and_size_types_negative_list)
def test_read_with_offset_and_size_negative(offset_type, size_type, key_and_data):
    """ Тест чтения с параметрами offset и size на negative тест-кейсах
    """
    key, data = key_and_data
    offset = et.OffsetReadGetter[offset_type](len(data))
    size = et.SizeGetter[size_type](len(data), offset)
    # подготавливаем данные для чтения
    elliptics_test.write_data(key, data)

    assert_that(calling(elliptics_test.read_data).with_args(key, offset=offset, size=size),
                raises(Exception, elliptics_test.errors.WrongArguments))

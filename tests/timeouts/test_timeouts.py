# -*- coding: utf-8 -*-
#
import pytest
import elliptics
import subprocess
import shlex
import time

from hamcrest import assert_that, calling, raises, less_than

import elliptics_testhelper as et

from elliptics_testhelper import key_and_data

config = pytest.config

class EllipticsTest(et.EllipticsTest):
    DROP_RULE = "INPUT --proto tcp --dport {port} --jump DROP"

    @staticmethod
    def drop_node(node):
        cmd = "ssh {host} iptables --append {rule}".format(host=node.host,
                                                           rule=EllipticsTest.DROP_RULE.format(port=node.port))
        subprocess.call(shlex.split(cmd))

    @staticmethod
    def resume_node(node):
        cmd = "ssh {host} iptables --delete {rule}".format(host=node.host,
                                                           rule=EllipticsTest.DROP_RULE.format(port=node.port))
        subprocess.call(shlex.split(cmd))

WRITE_TIMEOUT = config.getoption("write_timeout")
WAIT_TIMEOUT = config.getoption("wait_timeout")

elliptics_test = EllipticsTest(hosts=config.getoption("host"),
                               write_timeout=WRITE_TIMEOUT,
                               wait_timeout=WAIT_TIMEOUT,
                               groups=(1,))
@pytest.fixture()
def write_and_drop_node(request, key_and_data):
    key, data = key_and_data
    result = elliptics_test.write_data(key, data)
    node = result.storage_address
    elliptics_test.drop_node(node)

    def resume():
        elliptics_test.resume_node(node)

    request.addfinalizer(resume)
    return (key, node)

def test_wait_timeout(write_and_drop_node):
    key, node = write_and_drop_node

    EPS = 3.01
    start_time = time.time()
    assert_that(calling(elliptics_test.read_data).with_args(key),
                raises(Exception, 'Connection timed out'))
    exec_time = time.time() - start_time
    print(exec_time)

    assert_that(exec_time, less_than(WRITE_TIMEOUT + EPS))

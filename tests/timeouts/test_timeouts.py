# -*- coding: utf-8 -*-
#
import pytest
import elliptics
import subprocess
import shlex
import time
import random

from hamcrest import assert_that, calling, raises, less_than, greater_than, all_of

import elliptics_testhelper as et
import utils

from elliptics_testhelper import key_and_data
from utils import MB

config = pytest.config

class EllipticsTestHelper(et.EllipticsTestHelper):
    class Node(object):
        def __init__(self, host, port, group):
            self.host = host
            self.port = int(port)
            self.group = int(group)

    @staticmethod
    def get_nodes_from_args(args):
        return [EllipticsTestHelper.Node(*n.split(':')) for n in args]

    DROP_RULE = "INPUT --proto tcp --destination-port {port} --jump DROP"

    @staticmethod
    def drop_node(node):
        rule = EllipticsTestHelper.DROP_RULE.format(port=node.port)
        cmd = "ssh {host} iptables --append {rule}".format(host=node.host,
                                                           rule=rule)
        subprocess.call(shlex.split(cmd))

    @staticmethod
    def resume_node(node):
        rule = EllipticsTestHelper.DROP_RULE.format(port=node.port)
        cmd = "ssh {host} iptables --delete {rule}".format(host=node.host,
                                                           rule=rule)
        subprocess.call(shlex.split(cmd))

    @staticmethod
    def set_networking_limitations(download=9216, upload=9216):
        """ Sets download/upload bandwidth limitation (9 MBit)
        """
        cmd = "wondershaper eth0 {down} {up}".format(down=download, up=upload)
        subprocess.call(shlex.split(cmd))

    @staticmethod
    def clear_networking_limitations():
        cmd = "wondershaper clear eth0"
        subprocess.call(shlex.split(cmd))

WAIT_TIMEOUT = config.getoption("wait_timeout")
CHECK_TIMEOUT = config.getoption("check_timeout")
nodes = EllipticsTestHelper.get_nodes_from_args(config.getoption("node"))

@pytest.fixture(scope='function')
def test_helper():
    test_helper = EllipticsTestHelper(nodes=nodes,
                                      wait_timeout=WAIT_TIMEOUT,
                                      check_timeout=CHECK_TIMEOUT)
    return test_helper

@pytest.fixture(scope='function')
def write_and_drop_node(request, test_helper, key_and_data):
    key, data = key_and_data
    result = test_helper.write_data_now(key, data)
    node = result.storage_address
    test_helper.drop_node(node)

    def teardown():
        test_helper.resume_node(node)

    request.addfinalizer(teardown)
    return test_helper, key

def test_wait_timeout(write_and_drop_node):
    test_helper, key = write_and_drop_node

    # Additional 3 seconds for functions calling and networking stuff
    DELAY = 3
    start_time = time.time()
    assert_that(calling(test_helper.read_data_now).with_args(key),
                raises(elliptics.TimeoutError, EllipticsTestHelper.error_info.TimeoutError))
    exec_time = time.time() - start_time

    assert_that(all_of(exec_time, greater_than(WAIT_TIMEOUT),
                       exec_time, less_than(WAIT_TIMEOUT + DELAY)))

@pytest.fixture(scope='function')
def write_with_quorum_check(request, test_helper, key_and_data):
    # Data size depends on WAIT_TIMEOUT and networking limitations
    # (see EllipticsTestHelper.set_networking_limitations()).
    # Change this value depend on your network connection.
    size = 6*MB

    data = utils.get_data(size=size, randomize_len=False)
    key = utils.get_sha1(data)

    test_helper.set_checker(elliptics.checkers.quorum)

    test_helper.set_networking_limitations()
    res = test_helper.write_data(key, data)

    def teardown():
        test_helper.clear_networking_limitations()

    request.addfinalizer(teardown)
    
    return (test_helper, res)

@pytest.fixture(scope='function')
def quorum_checker_positive(request, write_with_quorum_check):
    test_helper, res = write_with_quorum_check
    node = random.choice(nodes)
    
    test_helper.drop_node(node)

    def teardown():
        test_helper.resume_node(node)

    request.addfinalizer(teardown)

    return res

@pytest.mark.groups_3
def test_quorum_checker_positive(quorum_checker_positive):
    async_result = quorum_checker_positive

    async_result.get()

@pytest.fixture(scope='function')
def quorum_checker_negative(request, write_with_quorum_check):
    test_helper, res = write_with_quorum_check
    dnodes = random.sample(nodes, 2)
    
    for node in dnodes:
        test_helper.drop_node(node)

    def teardown():
        for node in dnodes:
            test_helper.resume_node(node)

    request.addfinalizer(teardown)

    return res

@pytest.mark.groups_3
def test_quorum_checker_negative(quorum_checker_negative):
    async_result = quorum_checker_negative

    assert_that(calling(async_result.get),
                raises(elliptics.Error, EllipticsTestHelper.error_info.AddrNotExists))

@pytest.fixture(scope='function')
def write_and_shuffling_off(request, key_and_data):
    key, data = key_and_data
    
    # Clear groups shuffling flag
    config = elliptics.Config()
    config.flags &= ~elliptics.config_flags.mix_stats

    test_helper = EllipticsTestHelper(nodes=nodes,
                                      wait_timeout=WAIT_TIMEOUT,
                                      check_timeout=CHECK_TIMEOUT,
                                      config=config)

    test_helper.write_data_now(key, data)

    groups = random.sample(test_helper.get_groups(), 2)
    node = filter(lambda n: n.group == groups[0], nodes)[0]

    test_helper.drop_node(node)
    
    def teardown():
        test_helper.resume_node(node)

    request.addfinalizer(teardown)

    return (test_helper, key, groups)

@pytest.mark.groups_3
def test_read_from_groups(write_and_shuffling_off):
    test_helper, key, groups = write_and_shuffling_off
    
    start_time = time.time()
    test_helper.read_data_from_groups(key, groups).get()
    exec_time = time.time() - start_time

    assert_that(all_of(exec_time, greater_than(WAIT_TIMEOUT),
                       exec_time, less_than(WAIT_TIMEOUT * 2)))

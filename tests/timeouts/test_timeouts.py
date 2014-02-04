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

    DROP_RULE = "OUTPUT --destination {host} --proto tcp --dport {port} --jump DROP"

    @staticmethod
    def drop_node(node):
        cmd = "iptables --append {0}".format(EllipticsTestHelper.DROP_RULE.format(host=node.host,
                                                                                  port=node.port))
        subprocess.call(shlex.split(cmd))

    @staticmethod
    def resume_node(node):
        cmd = "iptables --delete {0}".format(EllipticsTestHelper.DROP_RULE.format(host=node.host,
                                                                                  port=node.port))
        subprocess.call(shlex.split(cmd))

WAIT_TIMEOUT = config.getoption("wait_timeout")
CHECK_TIMEOUT = config.getoption("check_timeout")
nodes = EllipticsTestHelper.get_nodes_from_args(config.getoption("node"))

elliptics_th = EllipticsTestHelper(nodes=nodes,
                                   wait_timeout=WAIT_TIMEOUT,
                                   check_timeout=CHECK_TIMEOUT)

@pytest.fixture(scope='function')
def write_and_drop_node(request, key_and_data):
    key, data = key_and_data
    result = elliptics_th.write_data_now(key, data)
    node = result.storage_address
    elliptics_th.drop_node(node)

    def teardown():
        elliptics_th.resume_node(node)

    request.addfinalizer(teardown)
    return (key, node)

def test_wait_timeout(write_and_drop_node):
    key, node = write_and_drop_node

    DELAY = 3.01
    start_time = time.time()
    assert_that(calling(elliptics_th.read_data_now).with_args(key),
                raises(elliptics.TimeoutError, EllipticsTestHelper.error_info.TimeoutError))
    exec_time = time.time() - start_time

    assert_that(all_of(exec_time, greater_than(WAIT_TIMEOUT),
                       exec_time, less_than(WAIT_TIMEOUT + DELAY)))

@pytest.fixture(scope='function')
def write_with_quorum_check(request, key_and_data):
    data = utils.get_data(size=700 * 2**20, randomize_len=False)
    key = utils.get_sha1(data)

    elliptics_th.set_checker(elliptics.checkers.quorum)

    res = elliptics_th.write_data(key, data)

    return res

@pytest.fixture(scope='function')
def quorum_checker_positive(request, write_with_quorum_check):
    node = random.choice(nodes)
    
    def teardown():
        elliptics_th.resume_node(node)

    request.addfinalizer(teardown)

    return (write_with_quorum_check, node)

@pytest.mark.groups_3
def test_quorum_checker_positive(quorum_checker_positive):
    async_result, node = quorum_checker_positive

    elliptics_th.drop_node(node)

    async_result.get()

@pytest.fixture(scope='function')
def quorum_checker_negative(request, write_with_quorum_check):
    dnodes = random.sample(nodes, 2)
    
    def teardown():
        for node in dnodes:
            elliptics_th.resume_node(node)

    request.addfinalizer(teardown)

    return (write_with_quorum_check, dnodes)

@pytest.mark.groups_3
def test_quorum_checker_negative(quorum_checker_negative):
    async_result, nodes = quorum_checker_negative

    for node in nodes:
        elliptics_th.drop_node(node)

    assert_that(calling(async_result.get),
                raises(elliptics.Error, EllipticsTestHelper.error_info.AddrNotExists))

@pytest.fixture(scope='function')
def write_and_shuffling_off(request, key_and_data):
    key, data = key_and_data
    
    global elliptics_th
    # Clear groups shuffling flag
    config = elliptics.Config()
    config.flags &= ~elliptics.config_flags.mix_stats

    et = EllipticsTestHelper(nodes=nodes,
                             wait_timeout=WAIT_TIMEOUT,
                             check_timeout=CHECK_TIMEOUT,
                             config=config)
    et, elliptics_th = elliptics_th, et

    elliptics_th.write_data_now(key, data)

    groups = random.sample(elliptics_th.get_groups(), 2)
    node = filter(lambda n: n.group == groups[0], nodes)[0]

    elliptics_th.drop_node(node)
    
    def teardown():
        global elliptics_th
        elliptics_th.resume_node(node)
        elliptics_th = et

    request.addfinalizer(teardown)

    return (key, groups)

@pytest.mark.groups_3
def test_read_from_groups(write_and_shuffling_off):
    key, groups = write_and_shuffling_off
    
    start_time = time.time()
    elliptics_th.read_data_from_groups(key, groups).get()
    exec_time = time.time() - start_time

    assert_that(all_of(exec_time, greater_than(WAIT_TIMEOUT),
                       exec_time, less_than(WAIT_TIMEOUT * 2)))

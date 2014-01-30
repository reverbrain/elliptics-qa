# -*- coding: utf-8 -*-
#
import pytest
import elliptics
import subprocess
import shlex
import time
import random

from hamcrest import assert_that, calling, raises, less_than

import elliptics_testhelper as et
import utils

from elliptics_testhelper import key_and_data

config = pytest.config

class EllipticsTest(et.EllipticsTest):
    class Node(object):
        def __init__(self, host, port):
            self.host = host
            self.port = port

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
HOSTS = config.getoption("host")

elliptics_test = EllipticsTest(hosts=HOSTS,
                               write_timeout=WRITE_TIMEOUT,
                               wait_timeout=WAIT_TIMEOUT,
                               groups=(1,))

@pytest.fixture(scope='function')
def write_and_drop_node(request, key_and_data):
    key, data = key_and_data
    result = elliptics_test.write_data(key, data)
    node = result.storage_address
    elliptics_test.drop_node(node)

    def resume():
        elliptics_test.resume_node(node)

    request.addfinalizer(resume)
    return (key, node)

@pytest.mark.groups_1
def test_wait_timeout(write_and_drop_node):
    key, node = write_and_drop_node

    DELAY = 3.01
    start_time = time.time()
    assert_that(calling(elliptics_test.read_data).with_args(key),
                raises(Exception, 'Connection timed out'))
    exec_time = time.time() - start_time

    assert_that(exec_time, less_than(WRITE_TIMEOUT + DELAY))

@pytest.fixture(scope='function')
def write_with_quorum_check(request, key_and_data):
    global elliptics_test
    elliptics_test = EllipticsTest(hosts=HOSTS,
                                   write_timeout=WRITE_TIMEOUT,
                                   wait_timeout=WAIT_TIMEOUT,
                                   groups=(1, 2, 3))
    data = utils.get_data(size=293 * 2**20, randomize_len=False)
    key = utils.get_sha1(data)

    elliptics_test.es.set_checker(elliptics.checkers.quorum)

    res = elliptics_test.es.write_data(key, data)

    return res

@pytest.fixture(scope='function')
def quorum_checker_positive(request, write_with_quorum_check):
    node = EllipticsTest.Node(host=HOSTS[random.randint(0, len(HOSTS) - 1)],
                              port=1025)
    
    def resume():
        elliptics_test.resume_node(node)

    request.addfinalizer(resume)

    return (write_with_quorum_check, node)

@pytest.mark.groups_3
def test_quorum_checker_positive(quorum_checker_positive):
    async_result, node = quorum_checker_positive

    elliptics_test.drop_node(node)

    async_result.get()

@pytest.fixture(scope='function')
def quorum_checker_negative(request, write_with_quorum_check):
    nodes = []
    hosts = HOSTS[:]
    for i in xrange(2):
        n = random.randint(0, len(hosts) - 1)
        node = EllipticsTest.Node(host=hosts[n],
                                  port=1025)
        nodes.append(node)
        del hosts[n]
    
    def resume():
        for node in nodes:
            elliptics_test.resume_node(node)

    request.addfinalizer(resume)

    return (write_with_quorum_check, nodes)

@pytest.mark.groups_3
def test_quorum_checker_negative(quorum_checker_negative):
    async_result, nodes = quorum_checker_negative

    for node in nodes:
        elliptics_test.drop_node(node)

    assert_that(calling(async_result.get),
                raises(Exception))

@pytest.fixture(scope='function')
def write_and_shuffling_off(request, key_and_data):
    key, data = key_and_data
    
    global elliptics_test
    config = elliptics.Config()
    config.flags &= ~elliptics.config_flags.mix_stats
    print('config', config.flags)
    elliptics_test = EllipticsTest(hosts=HOSTS,
                                   write_timeout=WRITE_TIMEOUT,
                                   wait_timeout=WAIT_TIMEOUT,
                                   groups=(1, 2, 3),
                                   config=config)

    elliptics_test.write_data(key, data)

    for h in HOSTS:
        if '3' in h:
            host = h
            break


    node = EllipticsTest.Node(host=host,
                              port=1025)

    elliptics_test.drop_node(node)
    
    def resume():
        elliptics_test.resume_node(node)

    request.addfinalizer(resume)

    return key
    

@pytest.mark.groups_3
def test_read_from_groups(write_and_shuffling_off):
    key = write_and_shuffling_off
    
    DELAY = 3.01
    start_time = time.time()
    elliptics_test.es.read_data_from_groups(key, (3, 2)).get()
    exec_time = time.time() - start_time
    print(exec_time)


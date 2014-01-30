# -*- coding: utf-8 -*-
#

def pytest_addoption(parser):
    parser.addoption('--write_timeout', type='int', default='20')
    parser.addoption('--wait_timeout', type='int', default='30')
    parser.addoption('--node', type='string', action='append')

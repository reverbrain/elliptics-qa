# -*- coding: utf-8 -*-

import sys
from os.path import abspath, dirname, join

BASE = dirname(dirname(dirname(abspath(__file__))))
sys.path.insert(0, join(BASE, 'lib'))

#TODO: заменить Exception на соответствующие исключения elliptics'а во всех тестах
def pytest_addoption(parser):
    parser.addoption('--write_timeout', type='int', default='20')
    parser.addoption('--wait_timeout', type='int', default='30')
    parser.addoption('--host', type='string', action='append')

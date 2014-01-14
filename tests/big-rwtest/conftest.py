def pytest_addoption(parser):
    parser.addoption('--batch_number', type='int', default='50')
    parser.addoption('--batch_size', type='int', default='50')
    parser.addoption('--write_timeout', type='int', default='50')
    parser.addoption('--wait_timeout', type='int', default='60')
    parser.addoption('--host', type='string', action='append')

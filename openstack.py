#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
import sys
import json
import subprocess
import shlex
import base64
import time
import requests
import signal
import socket

from collections import deque
from functools import wraps

LOGIN="zomb-prj-263"
PASS="3Iskdg4Ksgq"

TIMEOUT = 60

ENDPOINTS_INFO = {"COMPUTE": {'port': 8774,
                              'uri': {"IMAGES": 'v2/{tenant_id}/images',
                                      "FLAVORS": 'v2/{tenant_id}/flavors',
                                      "NETWORKS": 'v2/{tenant_id}/os-networks',
                                      "SERVERS": 'v2/{tenant_id}/servers',
                                      "SERVERS_SERVER": 'v2/{tenant_id}/servers/{instance_id}'}},
                  "IDENTITY": {'port': 5000,
                               'uri': {"TOKENS": 'v2.0/tokens'}}}

class TimeoutError(Exception):
    pass

def _alarm_handler(signal, frame):
    """ Хендлер для аларма (в случае, если падаем по таймауту в ожидании машинок,
    будем писать человеческое сообщение).
    """
    raise TimeoutError()

def with_timeout(func):
    def _decorator(self, *args, **kwargs):
        # устанавливаем таймаут
        signal.signal(signal.SIGALRM, _alarm_handler)
        signal.alarm(300) # 5 минут

        result = func(self, *args, **kwargs)

        # Отключаем таймер после получаения результата
        signal.alarm(0)

        return result
    return wraps(func)(_decorator)

class ApiRequestError(Exception):
    pass

class OpenstackSession:
    __HOST = "http://sas-openstack001.search.yandex.net"

    __USER_DATA = """#cloud-config
apt_preserve_sources_list: true
"""

    def __init__(self,
                 login=LOGIN,
                 password=PASS,
                 tenant_name="Devops"):
        self.tenant_name = tenant_name
        info = self.get_user_info(login, password)
        self.token_id = info['access']['token']['id']
        self.tenant_id = info['access']['token']['tenant']['id']

    def get(self, url):
        headers = {'Accept': "application/json",
                   'X-Auth-Project-Id': self.tenant_name,
                   'X-Auth-Token': self.token_id}

        r = requests.get(url, headers=headers, timeout=TIMEOUT)

        if r.status_code != requests.status_codes.codes.ok:
            raise ApiRequestError('Status code: {0}.\n{1}'.format(r.status_code, r.json()))
        
        return r.json()

    def post(self, url, data):
        headers = {'Content-Type': "application/json",
                   'Accept': "application/json"}
        if 'token_id' in dir(self):
            headers['X-Auth-Project-Id'] = self.tenant_name
            headers['X-Auth-Token'] = self.token_id

        r = requests.post(url, data=json.dumps(data), headers=headers, timeout=TIMEOUT)

        if r.status_code not in [requests.status_codes.codes.ok,
                                 requests.status_codes.codes.accepted]:
            raise ApiRequestError('Status code: {0}.\n{1}'.format(r.status_code, r.json()))
        
        return r.json()

    def delete(self, url):
        headers = {'Content-Type': "application/json",
                   'Accept': "application/json",
                   'X-Auth-Project-Id': self.tenant_name,
                   'X-Auth-Token': self.token_id}

        r = requests.delete(url, headers=headers, timeout=TIMEOUT)

        if r.status_code != 204:
            raise ApiRequestError('Status code: {0}.\n{1}'.format(r.status_code, r.json()))

    def _get_url(self, service_type, endpoint_type="COMPUTE", **kwargs):
        """ Возвращает адрес Service Endpoint по названию сервиса (endpoint_type)
        """
        url = '{host}:{port}/{uri}'.format(
            host=OpenstackSession.__HOST,
            port=ENDPOINTS_INFO[endpoint_type]['port'],
            uri=ENDPOINTS_INFO[endpoint_type]['uri'][service_type])
        url = url.format(**kwargs)
        return url

    def get_user_info(self, login=LOGIN, password=PASS):
        """ Возвращает информацию о пользователе
        """
        url = self._get_url("TOKENS", "IDENTITY")

        data = {'auth':
                    {'tenantName': self.tenant_name,
                     'passwordCredentials': {'username': login,
                                             'password': password}}}

        info = self.post(url, data)

        return info

    def get_images_list(self):
        """ Возвращает список образов
        """
        url = self._get_url("IMAGES", tenant_id=self.tenant_id)
        images_list = self.get(url)['images']
        return images_list

    def get_flavors_list(self):
        """ Возвращает список конфигураций (CPU's, RAM, disk space)
        """
        url = self._get_url("FLAVORS", tenant_id=self.tenant_id)
        flavors_list = self.get(url)['flavors']
        return flavors_list

    def get_networks_list(self):
        """ Возвращает список сетей
        """
        url = self._get_url("NETWORKS", tenant_id=self.tenant_id)
        networks_list = self.get(url)['networks']
        return networks_list

    def get_image_id(self, image_name):
        images_list = self.get_images_list()
        for image in images_list:
            if image['name'] == image_name:
                return image['id']
        return None

    def get_flavor_id(self, flavor_name):
        flavors_list = self.get_flavors_list()
        for flavor in flavors_list:
            if flavor['name'] == flavor_name:
                return flavor['id']
        return None

    def get_networks_uuid_list(self, networks_label_list):
        networks_list = self.get_networks_list()
        uuid_list = []
        for network in networks_list:
            if network['label'] in networks_label_list:
                uuid_list.append({"uuid": str(network['id'])})
        return uuid_list

    def _get_data_from_config(self, config):
        return {
            "server": {
                "name": config['name'],
                "imageRef": self.get_image_id(config['image_name']),
                "key_name": config['key_name'],
                "flavorRef": self.get_flavor_id(config['flavor_name']),
                "max_count": config['max_count'],
                "min_count": config['min_count'],
                "networks": self.get_networks_uuid_list(config['networks_label_list']),
                "user_data": base64.b64encode(OpenstackSession.__USER_DATA)
                }
            }

    def create_instance(self, data):
        data = self._get_data_from_config(data)
        url = self._get_url("SERVERS", tenant_id=self.tenant_id)

        instance_info = self.post(url, data)

        if instance_info.keys()[0] != 'server':
            for k, v in instance_info.items():
                print("{0}: {1}".format(k, v))
                
        return instance_info

    def get_instance_info(self, instance_name):
        # Получаем id инстанса
        created_instances = self.get_instances()
        for i in created_instances:
            if i['name'] == instance_name:
                instance_id = str(i['id'])
                break
        else:
            return None
                
        url = self._get_url("SERVERS_SERVER", tenant_id=self.tenant_id, instance_id=instance_id)
        instance = self.get(url)['server']
        
        return instance

    def delete_instance(self, instance_name):
        """ Удаляет инстанс в openstack'е
        """
        instance_info = self.get_instance_info(instance_name)
        if instance_info is None:
            print('There is no instance with such name: {0}'.format(instance_name))
            return False
        else:
            instance_id = instance_info['id']

        url = self._get_url("SERVERS_SERVER", tenant_id=self.tenant_id, instance_id=instance_id)

        self.delete(url)

    def get_instances(self):
        url = self._get_url("SERVERS", tenant_id=self.tenant_id)
        instances = self.get(url)['servers']
        return instances

    @with_timeout
    def _wait_till_active(self, instances):
        """ Ждет, когда все инстансы будут активны;
        возвращает список словарей {instance_name: ip}
        """
        print('Checking that instances are in ACTIVE state...')
        hosts_ip = {}
        queue = deque(instances)
        while queue:
            instance = queue.pop()
            instance_info = self.get_instance_info(instance)

            if instance_info['status'] == "ACTIVE":
                # запоминаем первый ip адрес
                network_name = instance_info['addresses'].keys()[0]
                hosts_ip[instance+'.i.fog.yandex.net'] = instance_info['addresses'][network_name][0]['addr']
                continue
            queue.appendleft(instance)
        return hosts_ip

    @with_timeout
    def _check_ssh_port(self, ip_list):
        """ Проверяет, что можем достучаться по ip по 22 порту
        """
        print('Checking ssh port...')
        queue = deque(ip_list)
        while queue:
            ip = queue.pop()
            # Проверяем доступность машины
            cmd = "nc -z -w1 {0} 22".format(ip)
            if subprocess.call(shlex.split(cmd)) == 0:
                continue
            # Возвращаем машину в очередь, если она еще не доступна
            queue.appendleft(ip)

    @with_timeout
    def _check_host_name_resolving(self, hosts_ip):
        """ Проверяем, что ip resolving возвращает те же ip адреса,
        что и API openstack'а
        """
        print('Checking host name resolving...')
        queue = deque(hosts_ip.items())
        while queue:
            host, ip = queue.pop()
            try:
                resolved_ip = socket.gethostbyname(host)
                if resolved_ip == ip:
                    continue
            except socket.error:
                pass
            queue.appendleft((host, ip))

    def check_availability(self, instances):
        """ Проверяет доступность инстансов openstack'а
        """
        print("Waiting for nodes to get online.")
        try:
            hosts_ip = self._wait_till_active(instances)
            self._check_ssh_port(hosts_ip.values())
            self._check_host_name_resolving(hosts_ip)
        except TimeoutError:
            return False
        else:
            return True

    def get_instances_names_from_conf(self, instance_cfg):
        """ Возвращает список имен ожидаемых созданных инстансов
        """
        name = instance_cfg['name']
        count = instance_cfg['max_count']
        # Формируем имена инстансов:
        if count == 1:
            instances = [name]
        else:
            # при не равном 1 значении максимального числа инстансов
            # добавляется суффикс -N, где N номер инстанса по порядку
            instances = [ name + '-' + str(i) for i in range(1, count + 1) ]
        return instances

if __name__ == "__main__":
    action = sys.argv[1] # create | delete
    config = json.load(open(sys.argv[2]))
    session = OpenstackSession()
    
    if action == 'delete':
        for instance_cfg in config['servers']:
            instances = session.get_instances_names_from_conf(instance_cfg)
            # Удаляем инстансы
            for i in instances:
                session.delete_instance(i)
    elif action == 'create':
        for instance_cfg in config['servers']:
            session.create_instance(data=instance_cfg)
        # проверяем доступность инстансов
        instances = []
        for instance_cfg in config['servers']:
            instances += session.get_instances_names_from_conf(instance_cfg)
        if not session.check_availability(instances):
            sys.exit('Not all nodes available')
    else:
        sys.exit("Wrong action '{0}'".format(action))

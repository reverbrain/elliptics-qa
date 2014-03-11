# -*- coding: utf-8 -*-
import pytest
import os
import glob
import json
import yaml
import sys
import re
import subprocess
import copy
import ConfigParser

sys.path.append(os.environ["UTILS_DIR"])
from openstack import main as openstack
from openstack import OpenstackSession

def pytest_addoption(parser):
    parser.addoption('--test', type="string", action="append", dest="test", default=None,
                     help='Example: --test="{test_name: {"test_params": {...}, "test_env_cfg": {...}}}')
    parser.addoption('--ansible-dir', type="string", action="store", dest="ansible_dir", default=None)
    parser.addoption('--packages-dir', type="string", action="store", dest="pkgs_dir", default=None)
    parser.addoption('--tests-dir', type="string", action="store", dest="tests_dir", default=None)
    parser.addoption('--tag', type="string", action="append", dest="tag", default=None)

@pytest.mark.tryfirst
def pytest_configure(config):
    pytest.testrunner = TestRunner(config)
    config.pluginmanager.register(pytest.testrunner)

@pytest.mark.trylast
def pytest_unconfigure(config):
    config.pluginmanager.unregister(pytest.testrunner)

class AnsibleManager(object):
    """ Class for managing ansible files and running playbooks
    (to prepare elliptics test environment)
    """
    # instances base names
    client_base_name = "elliptics-testing-client"
    node_base_name = "elliptics-testing-node"

    # ansible playbooks
    base_setup_playbook = "elliptics-setup"
    elliptics_start_stop_playbook = "elliptics-start-stop"

    # inventory groups
    localhost_group = 'localhost'
    clients_group = 'elliptics-client'
    nodes_group = 'elliptics'
    test_group = 'elliptics-test'

    def __init__(self, ansible_dir):
        self.ansible_dir = ansible_dir

    def get_vars(self, var_file):
        config = "{0}/{1}".format(self.ansible_dir, var_file)

        if not os.path.isfile(config):
            with open(config, "w") as f:
                f.write("{}\n")

        with open(config) as f:
            data = f.read()
            vars = yaml.load(data)

        return vars
        
    def set_vars(self, var_file, params):
        config = "{0}/{1}".format(self.ansible_dir, var_file)

        with open(config, 'w') as f:
            content = yaml.dump(params, explicit_start=True, default_flow_style=False)
            f.write(content)

    def update_vars(self, var_file, params):
        vars = self.get_vars(var_file)
        vars.update(params)
        self.set_vars(var_file, vars)

    def run_playbook(self, playbook):
        cmd = "ansible-playbook -v -i {ansible_dir}/{playbook}.hosts {ansible_dir}/{playbook}.yml"
        cmd = cmd.format(ansible_dir=self.ansible_dir, playbook=playbook)

        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)

        for line in iter(p.stdout.readline, ''):
            sys.stdout.write(line)

        p.wait()
        if p.returncode:
            raise RuntimeError("Playbook {0} failed (exit code: {1})".format(playbook, p.returncode))

    def generate_inventory_file(self, playbook, clients_count, nodes_in_group):
        inventory_host_record_template = '{0}.i.fog.yandex.net ansible_ssh_user=root'
        elliptics_group_template = 'elliptics-{0}'

        groups_count = len(nodes_in_group)
        nodes_count = sum(nodes_in_group)

        inventory = ConfigParser.ConfigParser(allow_no_value=True)

        # Add localhost
        inventory.add_section(self.localhost_group)
        host_record = "localhost ansible_connection=local"
        inventory.set(self.localhost_group, host_record)

        # Add elliptics clients
        inventory.add_section(self.clients_group)
        for i in self._get_host_names(base_name=self.client_base_name, count=clients_count):
            host_record = inventory_host_record_template.format(i)
            inventory.set(self.clients_group, host_record)

        # Add elliptics nodes
        nodes_names = self._get_host_names(base_name=self.node_base_name, count=nodes_count)
        node_name = (x for x in nodes_names)
        for g in xrange(groups_count):
            # per nodes_in_group nodes (at ansible group with elliptics-N name (N - number of group))
            group_name = elliptics_group_template.format(g + 1)
            inventory.add_section(group_name)
            for i in xrange(nodes_in_group[g]):
                host_record = inventory_host_record_template.format(next(node_name))
                inventory.set(group_name, host_record)

        # Group all node's groups in associated group
        nodes_group_defenition = self._as_group_of_groups(self.nodes_group)
        inventory.add_section(nodes_group_defenition)
        for g in xrange(groups_count):
            group_name = elliptics_group_template.format(g + 1)
            inventory.set(nodes_group_defenition, group_name)

        # Group clients and nodes in associated group
        test_group_defenition = self._as_group_of_groups(self.test_group)
        inventory.add_section(test_group_defenition)
        inventory.set(test_group_defenition, self.clients_group)
        inventory.set(test_group_defenition, self.nodes_group)

        with open('{0}/{1}.hosts'.format(self.ansible_dir, playbook), 'w') as inventory_file:
            inventory.write(inventory_file)

    @staticmethod
    def _as_group_of_groups(group):
        return '{0}:children'.format(group)

    @staticmethod
    def _get_host_names(base_name, count):
        config = {"name": base_name, "max_count": count}
        result = OpenstackSession.get_instances_names_from_conf(config)
        return result

class InstancesManager(object):
    """ Class for managing instances
    (to prepare elliptics test environment)
    """
    def __init__(self):
        self.delete_nodes = True

        self.flavors = {None: 0}
        for f in OpenstackSession().get_flavors_list():
            self.flavors[f['name']] = f['ram']

    def create(self, instances_cfg):
        openstack("create", instances_cfg)

    def delete(self, instances_cfg):
        openstack("delete", instances_cfg)

    def collect_instances_info(self, tests):
        """ Collect information about clients and nodes
        """
        instances_params = {"clients": {"count": 0, "flavor": None},
                            "nodes": {"count": 0, "flavor": None}}
        for test_cfg in tests.values():
            test_env = test_cfg["test_env_cfg"]
            for i in ["clients", "nodes"]:
                instances_params[i]["flavor"] = max(instances_params[i]["flavor"],
                                                    test_env[i]["flavor"],
                                                    key=self._flavors_order)
            instances_params["clients"]["count"] = max(instances_params["clients"]["count"],
                                                       test_env["clients"]["count"])
            instances_params["nodes"]["count"] = max(instances_params["nodes"]["count"],
                                                     sum(test_env["nodes"]["count_in_group"]))
            self.delete_nodes &= test_env["delete_nodes"]

        return instances_params

    def _flavors_order(self, f):
        """ Ordering function for instance flavor
        (ordering by RAM)
        """
        return self.flavors[f]

    @staticmethod
    def get_instances_cfg(instances_params, client_base_name, node_base_name):
        """ Prepares instances config for future usage
        """
        clients_conf = InstancesManager._get_cfg(client_base_name,
                                                 instances_params["clients"]["flavor"],
                                                 instances_params["clients"]["count"])
        nodes_conf = InstancesManager._get_cfg(node_base_name,
                                               instances_params["nodes"]["flavor"],
                                               instances_params["nodes"]["count"])

        return {"servers": [clients_conf, nodes_conf]}

    @staticmethod
    def _get_cfg(name, flavor, count):
        return {
            "name": name,
            "image_name": "elliptics",
            "key_name": "",
            "flavor_name": flavor,
            "max_count": count,
            "min_count": count,
            "networks_label_list": [
                "SEARCHOPENSTACKVMNETS"
                ]
            }

class TestRunner(object):
    def __init__(self, config):
        self.test_name = None
        self.prev_params_list = None

        self.pkgs_dir = config.option.pkgs_dir
        self.tests_dir = config.option.tests_dir

        self.ansible_manager = AnsibleManager(ansible_dir=config.option.ansible_dir)
        self.instances_manager = InstancesManager()

        self.tests = self.collect_tests(config.option.test, config.option.tag)
        self.instances_params = self.instances_manager.collect_instances_info(self.tests)
        self.instances_cfg = self.instances_manager.get_instances_cfg(self.instances_params,
                                                                      self.ansible_manager.client_base_name,
                                                                      self.ansible_manager.node_base_name)

    def run(self, name):
        playbook = self.tests[name]["playbook"]
        self.ansible_manager.run_playbook(playbook)
        
    # pytest hooks
    def pytest_configure(self, config):
        self.instances_manager.create(self.instances_cfg)
        # Installing elliptics packages on all clients and nodes
        self.ansible_manager.generate_inventory_file(playbook=self.ansible_manager.base_setup_playbook,
                                                     clients_count=self.instances_params["clients"]["count"],
                                                     nodes_in_group=[self.instances_params["nodes"]["count"]])
        if self.pkgs_dir:
            self.ansible_manager.set_vars(var_file="group_vars/all.yml",
                                          params={"packages_dir": self.pkgs_dir})
        self.ansible_manager.run_playbook(self.ansible_manager.base_setup_playbook)

    def pytest_unconfigure(self, config):
        if self.instances_manager.delete_nodes:
            self.instances_manager.delete(self.instances_cfg)

    def pytest_runtest_setup(self, item):
        self.test_name = re.split("[\[\]]", item.name)[1]

        self.prev_params_list = []

        for p_obj in self.tests[self.test_name]["params_list"]:
            # Saving previous vars files
            params = {"path": p_obj["path"],
                      "params": self.ansible_manager.get_vars(p_obj["path"])}
            self.prev_params_list.insert(0, params)
            
            self.ansible_manager.update_vars(p_obj["path"], p_obj["params"])

        test_env_cfg = self.tests[self.test_name]["test_env_cfg"]
        self.ansible_manager.generate_inventory_file(playbook=self.ansible_manager.elliptics_start_stop_playbook,
                                                     clients_count=test_env_cfg["clients"]["count"],
                                                     nodes_in_group=test_env_cfg["nodes"]["count_in_group"])
        self.ansible_manager.update_vars("group_vars/{0}.yml".format(self.ansible_manager.nodes_group),
                                         {"cmd": "start"})
        self.ansible_manager.run_playbook(self.ansible_manager.elliptics_start_stop_playbook)

        # Generate inventory-file for this test
        self.ansible_manager.generate_inventory_file(playbook=self.tests[self.test_name]["playbook"],
                                                     clients_count=test_env_cfg["clients"]["count"],
                                                     nodes_in_group=test_env_cfg["nodes"]["count_in_group"])

    def pytest_runtest_teardown(self, item):
        self.ansible_manager.update_vars("group_vars/{0}.yml".format(self.ansible_manager.nodes_group),
                                         {"cmd": "stop"})
        self.ansible_manager.run_playbook(self.ansible_manager.elliptics_start_stop_playbook)

        for p_obj in self.prev_params_list:
            self.ansible_manager.set_vars(p_obj["path"], p_obj["params"])
    #/pytest hooks

    def collect_tests(self, tests, tags):
        """ Prepares tests configs
        """
        collected_tests = {}
        if tests is None:
            # Collect all tests (with the "pull-request" tag)
            # if tests were not specified
            tests_dirs = [os.path.join(self.tests_dir, s) for s in os.listdir(self.tests_dir)
                          if os.path.isdir(os.path.join(self.tests_dir, s))]

            for test_dir in tests_dirs:
                for cfg_file in glob.glob(os.path.join(test_dir, "test_*.cfg")):
                    cfg = json.load(open(cfg_file), object_hook=self._decode_value)
                    if set(cfg["tags"]).intersection(set(tags)):
                        # test config name format: "test_NAME.cfg"
                        test_name = cfg_file.split('/')[-1][5:-4]
                        collected_tests[test_name] = cfg
        else:
            # Convert list of tests (from command line args)
            for t in [json.loads(t, object_hook=self._decode_value) for t in tests]:
                collected_tests.update(t)

        return collected_tests

    @staticmethod
    def _decode_list(data):
        res = []
        for i in data:
            res.append(TestRunner._decode_value(i))
        return res

    @staticmethod
    def _decode_object(data):
        res = {}
        for k, v in data.items():
            if isinstance(k, unicode):
                k = k.encode('utf-8')
            res[k] = TestRunner._decode_value(v)

        return res

    @staticmethod
    def _decode_value(data):
        res = data

        if isinstance(data, dict):
            res = TestRunner._decode_object(data)
        elif isinstance(data, list):
            res = TestRunner._decode_list(data)
        elif isinstance(data, unicode):
            res = data.encode('utf-8')

        return res

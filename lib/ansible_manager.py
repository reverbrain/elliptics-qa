import os
import sys
import yaml
import subprocess
import ConfigParser

import openstack

def get_vars(vars_path):
    if not os.path.isfile(vars_path):
        return {}

    with open(vars_path) as f:
        data = f.read()
        vars = yaml.load(data)

    return vars

def set_vars(vars_path, params):
    with open(vars_path, 'w') as f:
        content = yaml.dump(params, explicit_start=True, default_flow_style=False)
        f.write(content)

def update_vars(vars_path, params):
    vars = get_vars(vars_path)
    vars.update(params)
    set_vars(vars_path, vars)

def run_playbook(playbook, inventory=None):
    cmd = "ansible-playbook -v -i {0} {1}.yml".format(inventory, playbook)
    print("DEBUG: {0}".format(cmd))

    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)

    for line in iter(p.stdout.readline, ''):
        sys.stdout.write(line)

    p.wait()
    if p.returncode:
        raise RuntimeError("Playbook {0} failed (exit code: {1})".format(playbook, p.returncode))

def generate_inventory_file(inventory_path, clients_count, servers_per_group, groups, instances_names):
    inventory_host_record_template = '{0}.i.fog.yandex.net ansible_ssh_user=root'
    servers_group_template = 'servers-{0}'

    groups_count = len(servers_per_group)
    servers_count = sum(servers_per_group)

    inventory = ConfigParser.ConfigParser(allow_no_value=True)

    # Add elliptics clients
    inventory.add_section(groups["clients"])
    for i in _get_host_names(instance_name=instances_names['client'], count=clients_count):
        host_record = inventory_host_record_template.format(i)
        inventory.set(groups["clients"], host_record)
    # Add alias for clients' group (to use it in playbooks)
    clients_general_group = _as_group_of_groups('clients')
    inventory.add_section(clients_general_group)
    inventory.set(clients_general_group, groups["clients"])

    # Add elliptics servers
    servers_names = _get_host_names(instance_name=instances_names['server'], count=servers_count)
    server_name = (x for x in servers_names)
    for g in xrange(groups_count):
        # per servers_per_group[i] servers (at ansible group with servers-(i+1) name)
        group_name = servers_group_template.format(g + 1)
        inventory.add_section(group_name)
        for i in xrange(servers_per_group[g]):
            host_record = inventory_host_record_template.format(next(server_name))
            inventory.set(group_name, host_record)

    # Group all servers' groups in associated group
    servers_group_defenition = _as_group_of_groups(groups["servers"])
    inventory.add_section(servers_group_defenition)
    for g in xrange(groups_count):
        group_name = servers_group_template.format(g + 1)
        inventory.set(servers_group_defenition, group_name)
    # Add an alias for servers' group (to use it in playbooks)
    servers_general_group = _as_group_of_groups('servers')
    inventory.add_section(servers_general_group)
    inventory.set(servers_general_group, groups["servers"])

    # Group clients and servers in associated group
    test_group_defenition = _as_group_of_groups(groups["test"])
    inventory.add_section(test_group_defenition)
    inventory.set(test_group_defenition, groups["clients"])
    inventory.set(test_group_defenition, groups["servers"])
    # Add an alias for combining (servers and clients) group (to use it in playbooks)
    servers_general_group = _as_group_of_groups('test')
    inventory.add_section(servers_general_group)
    inventory.set(servers_general_group, groups["test"])

    with open(inventory_path, 'w') as inventory_file:
        inventory.write(inventory_file)

def _as_group_of_groups(group):
    return '{0}:children'.format(group)

def _get_host_names(instance_name, count):
    config = {"name": instance_name, "max_count": count}
    result = openstack.utils.get_instances_names_from_conf(config)
    return result

def _get_groups_names(name):
    groups = {"clients": "clients-{0}".format(name),
              "servers": "servers-{0}".format(name),
              "test": "test-{0}".format(name)}
    return groups

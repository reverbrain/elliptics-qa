#!/bin/bash -x

STATUS=1
PLAYBOOK=$1

export ANSIBLE_CONFIG=$WORKSPACE/ansible/ansible.cfg
export ANSIBLE_HOSTS=$WORKSPACE/ansible/$PLAYBOOK.hosts
if ansible -m ping all; then
    ansible-playbook $WORKSPACE/ansible/$PLAYBOOK.yml
    STATUS=$?
fi

exit $STATUS

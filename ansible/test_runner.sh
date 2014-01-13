#!/bin/bash -x

STATUS=1
PLAYBOOK=$1
DELETE_NODES=$2

if $WORKSPACE/openstack.py create $WORKSPACE/ansible/files/$PLAYBOOK.json; then
    export ANSIBLE_CONFIG=$WORKSPACE/ansible/ansible.cfg
    export ANSIBLE_HOSTS=$WORKSPACE/ansible/$PLAYBOOK.hosts
    if ansible -m ping all; then
        ansible-playbook $WORKSPACE/ansible/$PLAYBOOK.yml
        STATUS=$?
    fi
fi

if $DELETE_NODES || (( $STATUS==0 )); then
    $WORKSPACE/openstack.py delete $WORKSPACE/ansible/files/$PLAYBOOK.json || STATUS=1
fi

exit $STATUS

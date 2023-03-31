#!/bin/bash

# Constants
NEWLINE="echo """
PASSWORD="tolga.halit.batu"
USERNAME="pi_user"

# etcd Constants
CLIENT_PORT="2379"

# etcd Node Hosts
HOST_0="192.168.1.120"
HOST_1="192.168.1.121"
HOST_2="192.168.1.122"
HOST_3="192.168.1.123"
HOST_4="192.168.1.124"

# Commands
LIST="etcdctl --endpoints=${HOST_0}:${CLIENT_PORT},${HOST_1}:${CLIENT_PORT},${HOST_2}:${CLIENT_PORT} member list"
EXIT="exit"

# Script to execute
SCRIPT="$LIST;$EXIT"

# Parse arguments
NODE_NAME="PiRe-00"
HOSTNAME="192.168.1.120"

$NEWLINE
echo "---------------------------------"
echo "SSH Connection with $NODE_NAME"
echo "---------------------------------"
	
sshpass -p $PASSWORD ssh $USERNAME@$HOSTNAME $SCRIPT

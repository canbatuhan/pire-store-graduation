#!/bin/bash

NEWLINE="echo """
USERNAME="pi_user"
TEMPLATE="192.168.1.12"
PASSWORD="tolga.halit.batu"

CD="cd /home/batuhan/pire-store"
EXIT="exit"

for INDICATOR in {0..4} ; do
    NODE_NAME="PiRe-0$INDICATOR"
    HOSTNAME=$TEMPLATE$INDICATOR
    START="python main.py --id=$NODE_NAME"
    SCRIPT="$CD; $START & $EXIT"

	$NEWLINE
	echo "---------------------------------"
	echo "SSH Connection with $NODE_NAME"
	echo "---------------------------------"

	sshpass -p $PASSWORD ssh $USERNAME@$HOSTNAME $SCRIPT &
    wait 5
    echo "[Seagull Server Machine] > Node: $NODE_NAME is started."
done

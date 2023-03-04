#!/bin/bash

NEWLINE="echo """
PASSWORD="tolga.halit.batu"
USERNAME="pi_user"
TEMPLATE="192.168.1.12"

KILL="pkill -9 python" # Kills all python processes
EXIT="exit"
SCRIPT="$KILL;$EXIT"

for INDICATOR in {0..4} ; do
    NODE_NAME="PiRe-0$INDICATOR"
    HOSTNAME=$TEMPLATE$INDICATOR

	$NEWLINE
	echo "---------------------------------"
	echo "SSH Connection with $NODE_NAME"
	echo "---------------------------------"
	
    sshpass -p $PASSWORD ssh $USERNAME@$HOSTNAME $SCRIPT
    echo "[Seagull Server Machine] > All 'python' processes are killed."
done

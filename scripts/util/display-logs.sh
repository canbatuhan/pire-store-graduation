#!/bin/bash

NEWLINE="echo """
USERNAME="pi_user"
TEMPLATE="192.168.1.12"
PASSWORD="tolga.halit.batu"

CD="cd /home/batuhan/pire-store/pire/docs"
DISPLAY="sudo cat log.txt"
EXIT="exit"
SCRIPT="$CD;$DISPLAY;$EXIT"

for INDICATOR in {0..4} ; do
	NODE_NAME="PiRe-0$INDICATOR"
    HOSTNAME=$TEMPLATE$INDICATOR

	$NEWLINE
	echo "---------------------------------"
	echo "SSH Connection with $NODE_NAME"
	echo "---------------------------------"
	
    sshpass -p $PASSWORD ssh $USERNAME@$HOSTNAME $SCRIPT
done

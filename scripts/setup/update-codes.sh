#!/bin/bash

NEWLINE="echo """
USERNAME="pi_user"
TEMPLATE="192.168.1.12"
PASSWORD="tolga.halit.batu"

CD="cd /home/batuhan/pire-store"
SWITCH="sudo git switch cluster"
PULL="sudo git pull origin"
EXIT="exit"
SCRIPT="$CD;$SWITCH;$PULL;$EXIT"

for INDICATOR in {0..4} ; do
	NODE_NAME="PiRe-0$INDICATOR"
    HOSTNAME=$TEMPLATE$INDICATOR

	$NEWLINE
	echo "---------------------------------"
	echo "SSH Connection with $NODE_NAME..."
	echo "---------------------------------"

	sshpass -p $PASSWORD ssh $USERNAME@$HOSTNAME $SCRIPT
    echo "[Seagull Server Machine] > 'pire-store' codes are updated."
done

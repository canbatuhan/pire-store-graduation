#!/bin/bash

NEWLINE="echo """
PASSWORD="tolga.halit.batu"
USERNAME="pi_user"
TEMPLATE="192.168.1.12"

CD="cd /home/batuhan/pire-store/pire/docs"
CREATE_LOG="sudo touch log.txt"
CREATE_DB="sudo touch local.db"
PERMISSION="sudo chmod 777 *.*"
CLEAR_LOG="echo > log.txt"
CLEAR_DB="echo {} > local.db"
EXIT="exit"
SCRIPT="$CD;$CREATE_LOG;$CREATE_DB;$PERMISSION;$CLEAR_LOG;$CLEAR_DB;$EXIT"

for INDICATOR in {0..4} ; do
    NODE_NAME="PiRe-0$INDICATOR"
    HOSTNAME=$TEMPLATE$INDICATOR

	$NEWLINE
	echo "---------------------------------"
	echo "SSH Connection with $NODE_NAME"
	echo "---------------------------------"
	
    sshpass -p $PASSWORD ssh $USERNAME@$HOSTNAME $SCRIPT
    echo "[Seagull Server Machine] > 'log.txt' and 'local.db' are created."
done

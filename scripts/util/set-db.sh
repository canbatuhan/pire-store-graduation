#!/bin/bash

# Constants
NEWLINE="echo """
PASSWORD="tolga.halit.batu"
USERNAME="pi_user"
TEMPLATE="192.168.1.12"
DB_FILENAME="local.db"

# Commands
CD="cd /home/batuhan/pire-store/docs"
CREATE_DB="sudo touch $DB_FILENAME"
PERMISSION="sudo chmod 777 *.*"
CLEAR_DB="echo {} > $DB_FILENAME"
EXIT="exit"

# Script to execute
SCRIPT="$CD;$CREATE_DB;$PERMISSION;$CLEAR_DB;$EXIT"

# Parse arguments
while getopts :as:f: flag ; do
    case "${flag}" in
        a) START=0; FINISH=9;;
        s) START=${OPTARG};;
        f) FINISH=${OPTARG};;
    esac
done

# Clear 'log.txt' and 'local.db'
while [ $START -le $FINISH ] ; do
    NODE_NAME="pire-0$START"
    HOSTNAME=$TEMPLATE$START

	$NEWLINE
	echo "---------------------------------"
	echo "SSH Connection with $NODE_NAME"
	echo "---------------------------------"
	
    sshpass -p $PASSWORD ssh $USERNAME@$HOSTNAME $SCRIPT
    echo "[Seagull Server Machine] > '$DB_FILENAME' is set."
    START=$(($START+1))
done

#!/bin/bash

# Constants
NEWLINE="echo """
PASSWORD="tolga.halit.batu"
USERNAME="pi_user"
TEMPLATE="192.168.1.12"
LOG_FILENAME="log.txt"
DB_FILENAME="local.db"

# Commands
CD="cd /home/batuhan/pire-store/pire/docs"
DELETE_LOG="sudo rm $LOG_FILENAME"
DELETE_DB="sudo rm $DB_FILENAME"
EXIT="exit"

# Script to execute
SCRIPT="$CD;$DELETE_LOG;$DELETE_DB;$EXIT"

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
    NODE_NAME="PiRe-0$START"
    HOSTNAME=$TEMPLATE$START

	$NEWLINE
	echo "---------------------------------"
	echo "SSH Connection with $NODE_NAME"
	echo "---------------------------------"
	
    sshpass -p $PASSWORD ssh $USERNAME@$HOSTNAME $SCRIPT
    echo "[Seagull Server Machine] > '$LOG_FILENAME' and '$DB_FILENAME' are removed."
    START=$(($START+1))
done

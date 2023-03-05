#!/bin/bash

# Constants
NEWLINE="echo """
USERNAME="pi_user"
TEMPLATE="192.168.1.12"
PASSWORD="tolga.halit.batu"

# Commands
CD="cd /home/batuhan/pire-store"
SWITCH="sudo git switch cluster"
PULL="sudo git pull origin"
EXIT="exit"

# Script to execute
SCRIPT="$CD;$SWITCH;$PULL;$EXIT"

# Parse arguments
while getopts :as:f: flag ; do
    case "${flag}" in
        a) START=0; FINISH=9;;
        s) START=${OPTARG};;
        f) FINISH=${OPTARG};;
    esac
done

# Update pire-store
while [ $START -le $FINISH ] ; do
    NODE_NAME="PiRe-0$START"
    HOSTNAME=$TEMPLATE$START

	$NEWLINE
	echo "---------------------------------"
	echo "SSH Connection with $NODE_NAME..."
	echo "---------------------------------"

	sshpass -p $PASSWORD ssh $USERNAME@$HOSTNAME $SCRIPT
    echo "[Seagull Server Machine] > 'pire-store' codes are updated."
    START=$(($START+1))
done

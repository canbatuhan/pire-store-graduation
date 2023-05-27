#!/bin/bash

# Constants
NEWLINE="echo """
PASSWORD="tolga.halit.batu"
USERNAME="pi_user"
TEMPLATE="192.168.1.12"

# Commands
DOWNLOAD="sudo apt-get install python3-pip"
UPDATE="sudo python3.8 -m pip install --upgrade pip"
INSTALL="sudo python3.8 -m pip install grpcio protobuf==3.14.0 pickledb smpai"
EXIT="exit"

# Script to execute
SCRIPT="$UPDATE;$INSTALL;$EXIT"

# Parse arguments
while getopts :as:f: flag ; do
    case "${flag}" in
        a) START=0; FINISH=9; SCRIPT="$DOWNLOAD;$UPDATE;$INSTALL;$EXIT";
        s) START=${OPTARG};;
        f) FINISH=${OPTARG};;
    esac
done

# Install required modules
while [ $START -le $FINISH ] ; do
    NODE_NAME="PiRe-0$START"
    HOSTNAME=$TEMPLATE$START

	$NEWLINE
	echo "---------------------------------"
	echo "SSH Connection with $NODE_NAME"
	echo "---------------------------------"
	
    sshpass -p $PASSWORD ssh $USERNAME@$HOSTNAME $SCRIPT
    echo "[Seagull Server Machine] > Modules are installed/updated."
    START=$(($START+1))
done

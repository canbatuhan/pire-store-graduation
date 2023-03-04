#!/bin/bash

NEWLINE="echo """
PASSWORD="tolga.halit.batu"
USERNAME="pi_user"
TEMPLATE="192.168.1.12"

#UPDATE="sudo apt-get install python3-pip"
UPDATE="sudo python3.8 -m pip install --upgrade pip"
INSTALL="sudo python3.8 -m pip install grpcio protobuf==3.14.0 pickledb smpai"
EXIT="exit"
SCRIPT="$UPDATE;$INSTALL;$EXIT"

for INDICATOR in {0..4} ; do
    NODE_NAME="PiRe-0$INDICATOR"
    HOSTNAME=$TEMPLATE$INDICATOR

	$NEWLINE
	echo "---------------------------------"
	echo "SSH Connection with $NODE_NAME"
	echo "---------------------------------"
	
    sshpass -p $PASSWORD ssh $USERNAME@$HOSTNAME $SCRIPT
    echo "[Seagull Server Machine] > Modules are installed/updated."
done

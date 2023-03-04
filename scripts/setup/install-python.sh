#!/bin/bash

NEWLINE="echo """
PASSWORD="tolga.halit.batu"
USERNAME="pi_user"
TEMPLATE="192.168.1.12"

# Download phase
DOWNLOAD="sudo wget https://www.python.org/ftp/python/3.8.10/Python-3.8.10.tgz"
EXTRACT="sudo tar -zxvf Python-3.8.10.tgz"
DOWNLOAD_PHASE="$DOWNLOAD;$EXTRACT"

# Install phase
INSTALL_SSL="sudo apt install libssl-dev libncurses5-dev libsqlite3-dev libreadline-dev libtk8.6 libgdm-dev libdb4o-cil-dev libpcap-dev"
CD2PYTHON="cd Python-3.8.10"
ENABLE="./configure --enable-optimizations"
MAKE="sudo make"
INSTALL="sudo make altinstall"
INSTALL_PHASE="$INSTALL_SSL;$CD2PYTHON;$ENABLE;$MAKE;$INSTALL"

# Setup phase
CD2BIN="cd /usr/bin"
REMOVE="sudo rm python"
SET="sudo ln -s /usr/local/bin/python3.8 python"
PIP="sudo apt-get install python3-pip"
CHECK="python --version"
EXIT="exit"
SETUP_PHASE="$CD2BIN;$REMOVE;$SET;$PIP;$CHECK;$EXIT"

#SCRIPT="$DOWNLOAD_PHASE;$INSTALL_PHASE;$SETUP_PHASE"
SCRIPT="$INSTALL_SSL;$CD2PYTHON;$ENABLE;$MAKE;$INSTALL;$EXIT"

for INDICATOR in {0..4} ; do
    NODE_NAME="PiRe-0$INDICATOR"
    HOSTNAME=$TEMPLATE$INDICATOR

	$NEWLINE
	echo "---------------------------------"
	echo "SSH Connection with $NODE_NAME"
	echo "---------------------------------"
	
    sshpass -p $PASSWORD ssh $USERNAME@$HOSTNAME $SCRIPT
    echo "[Seagull Server Machine] > Python 3.8.10 installed."
done

#!/bin/bash

# Constants
NEWLINE="echo """
USERNAME="pi_user"
TEMPLATE="192.168.1.12"
PASSWORD="tolga.halit.batu"

# Commands
CD="cd /home/batuhan/pire-store"
SLEEP="sleep 1"
EXIT="exit"

# Parse positional arguments
COMMAND=$1
KEY=$2
VALUE=$3

# Socket settings
SERVERS=(0 1 2 3 4)
SEED=$RANDOM%${#SERVERS[@]}
RANDOM_SERVER=${SERVERS[$SEED]}
HOST="$TEMPLATE$RANDOM_SERVER"
PORT=9000
MESSAGE="exit"

# create(x,42) or update(x,42)
if [ "$COMMAND" = "CREATE" ] || [ "$COMMAND" = "UPDATE" ] ; then
    if [ -n "$VALUE" ] ; then
        MESSAGE="{\"command\":\"$COMMAND\", \"key\":\"$KEY\", \"value\":\"$VALUE\"}"
    fi
fi

# read(x) or delete(x)
if [ "$COMMAND" = "READ" ] || [ "$COMMAND" = "DELETE" ] ; then
    if [ -z "$VALUE" ] ; then
        MESSAGE="{\"command\":\"$COMMAND\", \"key\":\"$KEY\"}"
    fi
fi

# Send message
echo "Sending $MESSAGE to $HOST:$PORT"
echo -n $MESSAGE | netcat $HOST $PORT




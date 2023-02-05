echo "-----------------------------------"
echo "-- Test User Interface for Pi-Re --"
echo "-----------------------------------"

PORTS=(8005 8015 8025 8035 8045)
SEED=$RANDOM%${#PORTS[@]}

RUNNER=python
PROGRAM=pire/docs/examples/single_cmd.py
HOST=127.0.0.1

while :
do
    RANDOM_PORT=${PORTS[$SEED]}
    read -p "pire > " USER_CMD
    ARGS="--host=$HOST --port=$RANDOM_PORT --cmd=$USER_CMD"
    $RUNNER $PROGRAM $ARGS
done
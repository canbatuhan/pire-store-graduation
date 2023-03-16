ABS_CLUSTER_PATH="/home/batuhanc/Documents/pire-store/scripts/cluster"

$ABS_CLUSTER_PATH/send.sh create a 42 &
sleep 0.05 ; $ABS_CLUSTER_PATH/send.sh create b 84 &
sleep 0.05 ; $ABS_CLUSTER_PATH/send.sh create c 21 &
sleep 0.05 ; $ABS_CLUSTER_PATH/send.sh create d 63 &
sleep 0.05 ; $ABS_CLUSTER_PATH/send.sh create e 105


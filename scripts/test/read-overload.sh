ABS_CLUSTER_PATH="/home/batuhanc/Documents/pire-store/scripts/cluster"

COUNT=0
N_ITERATIONS=4
while [ $COUNT -le $N_ITERATIONS ] ; do
    $ABS_CLUSTER_PATH/send.sh read a &
    $ABS_CLUSTER_PATH/send.sh read b &
    $ABS_CLUSTER_PATH/send.sh read c &
    $ABS_CLUSTER_PATH/send.sh read d &
    $ABS_CLUSTER_PATH/send.sh read e &
    COUNT=$(($COUNT+1))
done
sleep 2

ABS_CLUSTER_PATH="/home/batuhanc/Documents/pire-store/scripts/cluster"

COUNT=0
N_ITERATIONS=2
while [ $COUNT -le $N_ITERATIONS ] ; do
    $ABS_CLUSTER_PATH/send.sh read a &
    $ABS_CLUSTER_PATH/send.sh read b &
    $ABS_CLUSTER_PATH/send.sh read c &
    $ABS_CLUSTER_PATH/send.sh read d &
    $ABS_CLUSTER_PATH/send.sh read e &
    $ABS_CLUSTER_PATH/send.sh read f &
    $ABS_CLUSTER_PATH/send.sh read g &
    $ABS_CLUSTER_PATH/send.sh read h &
    $ABS_CLUSTER_PATH/send.sh read i &
    $ABS_CLUSTER_PATH/send.sh read j &
    $ABS_CLUSTER_PATH/send.sh read k &
    $ABS_CLUSTER_PATH/send.sh read l &
    $ABS_CLUSTER_PATH/send.sh read m &
    $ABS_CLUSTER_PATH/send.sh read n &
    $ABS_CLUSTER_PATH/send.sh read o &
    $ABS_CLUSTER_PATH/send.sh read p &
    $ABS_CLUSTER_PATH/send.sh read r &
    $ABS_CLUSTER_PATH/send.sh read s &
    $ABS_CLUSTER_PATH/send.sh read t &
    $ABS_CLUSTER_PATH/send.sh read u &
    $ABS_CLUSTER_PATH/send.sh read v &
    $ABS_CLUSTER_PATH/send.sh read w &
    $ABS_CLUSTER_PATH/send.sh read x &
    $ABS_CLUSTER_PATH/send.sh read y &
    $ABS_CLUSTER_PATH/send.sh read z &
    COUNT=$(($COUNT+1))
done
sleep 1

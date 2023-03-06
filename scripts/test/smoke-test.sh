ABS_CLUSTER_PATH="/home/batuhanc/Documents/pire-store/scripts/cluster"

$ABS_CLUSTER_PATH/send.sh create x 42; echo ""
$ABS_CLUSTER_PATH/send.sh read x; echo ""
$ABS_CLUSTER_PATH/send.sh update x 21; echo ""
$ABS_CLUSTER_PATH/send.sh read x; echo ""
$ABS_CLUSTER_PATH/send.sh delete x; echo ""
$ABS_CLUSTER_PATH/send.sh read x; echo ""

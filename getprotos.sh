echo "---------------------------------"
echo "-- Code generator for protobuf --"
echo "---------------------------------"

RUNNER=python
FLAGS=-m
PROGRAM=grpc_tools.protoc
PROTO_DIR_PATH=./pire/docs/protos
PYTHON_OUT=./pire/modules/service
GRPC_PYTHON_OUT=./pire/modules/service
PROTO_FILE_PATH=./pire/docs/protos/pirestore.proto

$RUNNER $FLAGS $PROGRAM --proto_path=$PROTO_DIR_PATH --python_out=$PYTHON_OUT --grpc_python_out=$GRPC_PYTHON_OUT $PROTO_FILE_PATH
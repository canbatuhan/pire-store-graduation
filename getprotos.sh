python -m grpc_tools.protoc --proto_path=./pire/docs/protos --python_out=./pire/modules/server --grpc_python_out=./pire/modules/server ./pire/docs/protos/pirestore.proto
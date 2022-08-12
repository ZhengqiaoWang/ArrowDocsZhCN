

GRPC_CPP_PLUGIN=grpc_cpp_plugin
GRPC_CPP_PLUGIN_PATH=`which ${GRPC_CPP_PLUGIN}`

echo ${GRPC_CPP_PLUGIN_PATH}

protoc-3.21.4.0 --cpp_out proto/ -I proto/ --grpc_out=proto/ --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN_PATH} proto/arrow_grpc.service.proto
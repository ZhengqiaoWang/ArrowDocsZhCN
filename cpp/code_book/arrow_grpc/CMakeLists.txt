project(DemoArrowGRPC)

find_package(Arrow REQUIRED)
message(STATUS "Using gRPC ${gRPC_VERSION}")
find_package(Protobuf 3 REQUIRED)
message(STATUS "Using protobuf ${Protobuf_VERSION}")

set(CMAKE_INCLUDE_CURRENT_DIR ON)
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

SET(MESSAGE_SRC ${CMAKE_SOURCE_DIR}/proto)
SET(MESSAGE_DIR ${CMAKE_SOURCE_DIR}/proto)

add_executable(service service.cpp
    proto/arrow_grpc.service.grpc.pb.cc
    proto/arrow_grpc.service.grpc.pb.h
    proto/arrow_grpc.service.pb.cc
    proto/arrow_grpc.service.pb.h
    )
target_link_libraries(service PRIVATE arrow_shared)
target_link_libraries(service PRIVATE parquet)
target_link_libraries(service PRIVATE protobuf)
target_link_libraries(service PRIVATE grpc++)
target_include_directories(service 
    PRIVATE 
    $<BUILD_INTERFACE:${MESSAGE_DIR}>)
    
add_executable(client client.cpp
    proto/arrow_grpc.service.grpc.pb.cc
    proto/arrow_grpc.service.grpc.pb.h
    proto/arrow_grpc.service.pb.cc
    proto/arrow_grpc.service.pb.h)
target_link_libraries(client PRIVATE arrow_shared)
target_link_libraries(client PRIVATE parquet)
target_link_libraries(client PRIVATE dl)
target_link_libraries(client PRIVATE protobuf)
target_link_libraries(client PRIVATE grpc++)
target_include_directories(client 
    PRIVATE 
    ${MESSAGE_DIR})

add_executable(data_builder data_builder.cpp)
target_link_libraries(data_builder PRIVATE arrow_shared)
target_link_libraries(data_builder PRIVATE parquet)
# target_link_libraries(data_builder PRIVATE grpc++)

add_definitions("-Wall -O2 --std=c++11")

project(DemoArrowGRPC)

find_package(Arrow REQUIRED)
find_package(Protobuf 3 REQUIRED)

SET(MESSAGE_SRC ${CMAKE_SOURCE_DIR}/proto)
SET(MESSAGE_DIR ${CMAKE_SOURCE_DIR}/proto)

add_executable(service service.cpp)
target_link_libraries(service PRIVATE arrow_shared)
target_link_libraries(service PRIVATE parquet)
target_link_libraries(service PRIVATE grpc)
target_sources(service PRIVATE ${MESSAGE_SRC})
target_include_directories(service 
    PRIVATE 
    $<BUILD_INTERFACE:${Protobuf_INCLUDE_DIRS}>
    $<BUILD_INTERFACE:${MESSAGE_DIR}>)
target_link_libraries(service PRIVATE ${Protobuf_LIBRARIES})
    
add_executable(client client.cpp)
target_link_libraries(client PRIVATE arrow_shared)
target_link_libraries(client PRIVATE parquet)
target_link_libraries(client PRIVATE grpc)
target_sources(client PRIVATE ${MESSAGE_SRC})
target_include_directories(client 
    PRIVATE 
    $<BUILD_INTERFACE:${Protobuf_INCLUDE_DIRS}>
    $<BUILD_INTERFACE:${MESSAGE_DIR}>)
target_link_libraries(client PRIVATE ${Protobuf_LIBRARIES})

add_executable(data_builder data_builder.cpp)
target_link_libraries(data_builder PRIVATE arrow_shared)
target_link_libraries(data_builder PRIVATE parquet)
target_link_libraries(data_builder PRIVATE grpc)

add_definitions("-Wall -O2 --std=c++11")
project(DemoFlight)

find_package(Arrow REQUIRED)

add_executable(service service.cpp)
target_link_libraries(service PRIVATE arrow_shared)
target_link_libraries(service PRIVATE parquet)
target_link_libraries(service PRIVATE arrow_flight)
target_link_libraries(service PRIVATE grpc)

add_executable(client client.cpp)
target_link_libraries(client PRIVATE arrow_shared)
target_link_libraries(client PRIVATE parquet)
target_link_libraries(client PRIVATE arrow_flight)
target_link_libraries(client PRIVATE grpc)

add_executable(data_builder data_builder.cpp)
target_link_libraries(data_builder PRIVATE arrow_shared)
target_link_libraries(data_builder PRIVATE parquet)
target_link_libraries(data_builder PRIVATE arrow_flight)
target_link_libraries(data_builder PRIVATE grpc)

add_definitions("-Wall -O2 --std=c++11")    
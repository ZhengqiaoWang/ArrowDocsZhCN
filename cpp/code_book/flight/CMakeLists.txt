project(DemoFlight)

find_package(Arrow REQUIRED)

add_executable(service service.cpp)
target_link_libraries(service PRIVATE arrow_shared)
target_link_libraries(service PRIVATE parquet)
target_link_libraries(service PRIVATE arrow_flight)
target_link_libraries(service PRIVATE grpc)

add_executable(uploader uploader.cpp)
target_link_libraries(uploader PRIVATE arrow_shared)
target_link_libraries(uploader PRIVATE parquet)
target_link_libraries(uploader PRIVATE arrow_flight)
target_link_libraries(uploader PRIVATE grpc)



add_definitions("-Wall -g")
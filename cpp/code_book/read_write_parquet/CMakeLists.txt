project(DemoReadPartitionedData)

find_package(Arrow REQUIRED)

add_executable(arrow_table arrow_table.cpp)
add_executable(stream stream.cpp)
target_link_libraries(arrow_table PRIVATE arrow_shared)
target_link_libraries(arrow_table PRIVATE parquet)

target_link_libraries(stream PRIVATE arrow_shared)
target_link_libraries(stream PRIVATE parquet)

add_definitions("-Wall -g")
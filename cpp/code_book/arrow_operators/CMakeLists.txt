project(DemoArrowOper)

find_package(Arrow REQUIRED)
message(STATUS "Using gRPC ${gRPC_VERSION}")

set(CMAKE_INCLUDE_CURRENT_DIR ON)
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# make_parquet
add_executable(make_parquet make_parquet.cpp)
target_link_libraries(make_parquet PRIVATE arrow_shared)
target_link_libraries(make_parquet PRIVATE arrow_dataset)
target_link_libraries(make_parquet PRIVATE parquet)

# scan_whole_datasets
add_executable(scan_whole_datasets scan_whole_datasets.cpp)
target_link_libraries(scan_whole_datasets PRIVATE arrow_shared)
target_link_libraries(scan_whole_datasets PRIVATE arrow_dataset)
target_link_libraries(scan_whole_datasets PRIVATE parquet)

# make_feather
add_executable(make_feather make_feather.cpp)
target_link_libraries(make_feather PRIVATE arrow_shared)
target_link_libraries(make_feather PRIVATE arrow_dataset)
target_link_libraries(make_feather PRIVATE parquet)

# filter_select_dataset
add_executable(filter_select_dataset filter_select_dataset.cpp)
target_link_libraries(filter_select_dataset PRIVATE arrow_shared)
target_link_libraries(filter_select_dataset PRIVATE arrow_dataset)
target_link_libraries(filter_select_dataset PRIVATE parquet)

# project_dataset
add_executable(project_dataset project_dataset.cpp)
target_link_libraries(project_dataset PRIVATE arrow_shared)
target_link_libraries(project_dataset PRIVATE arrow_dataset)
target_link_libraries(project_dataset PRIVATE parquet)

# make_partition_parquet
add_executable(make_partition_parquet make_partition_parquet.cpp)
target_link_libraries(make_partition_parquet PRIVATE arrow_shared)
target_link_libraries(make_partition_parquet PRIVATE arrow_dataset)
target_link_libraries(make_partition_parquet PRIVATE parquet)

# scan_partition_datasets
add_executable(scan_partition_datasets scan_partition_datasets.cpp)
target_link_libraries(scan_partition_datasets PRIVATE arrow_shared)
target_link_libraries(scan_partition_datasets PRIVATE arrow_dataset)
target_link_libraries(scan_partition_datasets PRIVATE parquet)

add_definitions("-Wall -O2 --std=c++11")

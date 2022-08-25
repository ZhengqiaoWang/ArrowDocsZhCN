project(DemoArrowOper)

find_package(Arrow REQUIRED)
message(STATUS "Using gRPC ${gRPC_VERSION}")

set(CMAKE_INCLUDE_CURRENT_DIR ON)
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# compute_funcs
add_executable(compute_funcs compute_funcs.cpp)
target_link_libraries(compute_funcs PRIVATE arrow_shared)
target_link_libraries(compute_funcs PRIVATE arrow_dataset)
target_link_libraries(compute_funcs PRIVATE parquet)

add_definitions("-Wall -O2 --std=c++11")

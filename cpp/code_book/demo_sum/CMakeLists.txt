project(DemoSum)

find_package(Arrow REQUIRED)

add_executable(demo_sum main.cpp)
target_link_libraries(demo_sum PRIVATE arrow_shared)

project(DemoRandomDataSchema)

find_package(Arrow REQUIRED)

add_executable(demo main.cpp)
target_link_libraries(demo PRIVATE arrow_shared)

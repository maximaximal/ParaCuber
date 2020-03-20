set(TEST_SRCS
    ${CMAKE_CURRENT_SOURCE_DIR}/paracooba-test-main.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/asio-test.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/cnftree-test.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/path_factory_test.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/assignment_serializer_test.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/cuber_test.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/connection_test.cpp
    )

add_executable(parac-test ${TEST_SRCS})

set_property(TARGET parac-test PROPERTY CXX_STANDARD 17)

target_link_libraries(parac-test paracooba Catch2::Catch2 cereal)
catch_discover_tests(parac-test)

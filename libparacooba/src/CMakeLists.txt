add_subdirectory(cuber)
add_subdirectory(webserver)
add_subdirectory(messages)
add_subdirectory(net)

set(PARACOOBA_SRCS
    ${CMAKE_CURRENT_SOURCE_DIR}/communicator.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/cluster-statistics.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/runner.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/config.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/log.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/task_factory.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/task.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/taskresult.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/client.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/daemon.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/cnf.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/cnftree.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/util.cpp

    ${CUBER_SRCS}
    ${WEBSERVER_SRCS}
    ${MESSAGES_SRCS}
    ${NET_SRCS}

    ${CMAKE_CURRENT_SOURCE_DIR}/cadical_task.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/decision_task.cpp

    ${CMAKE_CURRENT_SOURCE_DIR}/cadical_mgr.cpp
    PARENT_SCOPE)

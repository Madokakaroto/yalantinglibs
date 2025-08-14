set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/output/examples/coro_rpc)

add_executable(coro_rpc_rdma_example_win
    rdma_example_win.cpp
  )

target_include_directories(coro_rpc_rdma_example_win PUBLIC ${YLT_NETWORKDIRECT_INCLUDE_DIR})
target_link_directories(coro_rpc_rdma_example_win PUBLIC ${YLT_NETWORKDIRECT_LIB_DIR})
target_link_libraries(coro_rpc_rdma_example_win ndutil.lib)

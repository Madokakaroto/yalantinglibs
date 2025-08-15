#pragma once

#include <asio/detail/winsock_init.hpp>
#ifdef _MSC_VER
  #pragma warning(push)
  #pragma warning(disable:4073)
  #pragma init_seg(lib)
  asio::detail::winsock_init<>::manual manual_winsock_init;
  #pragma warning(pop)
#else // using MinGw (gcc)
  asio::detail::winsock_init<>::manual manual_winsock_init
    __attribute__ ((init_priority (101)));
#endif
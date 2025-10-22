#pragma once

#include "asio/detail/config.hpp"
#include "asio/detail/object_pool.hpp"
#include "asio/detail/io_object_impl.hpp"
#include "asio/execution_context.hpp"
#include "asio/detail/win_iocp_io_context.hpp"
#include "ylt/coro_io/networkdirect/nd_device.hpp"

namespace coro_io {

class nd2_service_base {
public:
  struct base_implementation_type {
   nd_device_t* device_;
   base_implementation_type* next_;
   base_implementation_type* prev_;
 };

protected:
 asio::detail::win_iocp_io_context& scheduler_;
 asio::detail::mutex mutex_;
 base_implementation_type* impl_list_;
};

}
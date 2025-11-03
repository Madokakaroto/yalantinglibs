#pragma once

#include <memory>
#include <string>
#include <array>
#include <numeric>
#include <ranges>
#include <iterator>
#include <winnt.h>
#include <wrl/client.h>
#include <libloaderapi.h>
#include <ws2spi.h>
#include <guiddef.h>
#include <ndsupport.h>
#include <ndstatus.h>
#include <ndspi.h>

namespace coro_io {

using size_type = ULONG;
using result_type = HRESULT;

// command types
enum mr_acccess_flag_t {
  mr_access_local_write,
  mr_access_remote_read,
  mr_access_remote_write,
};

// configuration type to initialize the shared state
// TODO ... align with ibverbs
struct nd_connector_config_t {
  size_type poll_wc_count = 4;
  size_type cqe_ = 64;
  size_type max_send_wr_ = 32;
  size_type max_recv_wr_ = 32;
  size_type max_send_sge_ = 8;
  size_type max_recv_sge_ = 8;
  size_type max_inline_data_ = 16;
  size_type inbound_read_limit_ = 0;
  size_type outbound_read_limit_ = 0;
};

struct nd_remote_addr_t {
  std::uint64_t addr_;
  std::uint32_t token_;
};

}

// types not used directly
#include "ylt/coro_io/networkdirect/detail/nd_impl_types.hpp"

namespace coro_io {

// nd device
using nd_context_config_t = detail::native_context_config_t;
using nd_device_t = detail::native_device_t;
using nd_device_ptr = detail::native_device_ptr;

}  // namespace coro_io
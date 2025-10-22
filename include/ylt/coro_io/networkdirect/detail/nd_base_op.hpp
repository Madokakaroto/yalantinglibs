#pragma once

#include "asio/detail/operation.hpp"
#include "asio/detail/win_iocp_io_context.hpp"
#include "ylt/coro_io/networkdirect/nd_types.hpp"

namespace coro_io::detail {

class nd_op_base : public asio::detail::operation {
public:
  enum class status_t {
    completed,
    in_completed,
  };

private:

};

}
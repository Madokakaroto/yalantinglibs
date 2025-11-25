#pragma once

#include "asio/buffer.hpp"
#include "async_simple/Executor.h"
#include "async_simple/Future.h"
#include "async_simple/Promise.h"
#include "async_simple/Signal.h"
#include "async_simple/coro/Collect.h"
#include "async_simple/coro/FutureAwaiter.h"
#include "async_simple/coro/Lazy.h"
#include "ylt/coro_io/coro_io.hpp"
#include "ylt/coro_io/io_context_pool.hpp"
#include "ylt/easylog.hpp"
#include "ylt/struct_pack.hpp"
#include "ylt/struct_pack/reflection.hpp"
#include "ylt/coro_io/networkdirect/nd_error.hpp"
#include "ylt/coro_io/networkdirect/nd_buffer.hpp"
#include "ylt/coro_io/networkdirect/nd_device.hpp"
#include "ylt/coro_io/networkdirect/nd_connection.hpp"
#include "ylt/coro_io/networkdirect/nd_listener.hpp"
#include "ylt/coro_io/networkdirect/nd_socket.hpp"
#include "ylt/coro_io/networkdirect/nd_portspace.hpp"

namespace coro_io {

template <typename PortSpace, typename Endpoint>
inline async_simple::coro::Lazy<std::error_code> async_connect(
    nd_connection<PortSpace>& connection, Endpoint endpoint) {
  if (!connection.has_executor()) {
    co_return nd_errc::ext_no_executor;
  }
  auto ec = co_await async_io<std::error_code>(
      [&](auto cb) {
        connection.async_connect(endpoint, cb);
      },
      connection);
  co_return ec;
}

template <typename PortSpace>
inline async_simple::coro::Lazy<std::error_code> async_accept(
    nd_listener<PortSpace>& listener, nd_connection<PortSpace>& connection,
    nd_config_t const& config = nd_config_t{}) {
  if (!listener.has_executor()) {
    co_return nd_errc::ext_no_executor;
  }
  auto ec = co_await async_io<std::error_code>(
      [&](auto cb) {
        listener.async_accept(connection, config, cb);
      },
      listener);
  co_return ec;
}

template <typename PortSpace, typename MutableBufferSequence>
inline async_simple::coro::Lazy<std::pair<std::error_code, std::size_t>>
async_recv(nd_connection<PortSpace>& connection,
           MutableBufferSequence const& buffers) {
  if (!connection.has_executor()) {
    co_return std::make_pair(make_error_code(nd_errc::ext_no_executor),
                             std::size_t{0});
  }

  auto result = co_await async_io<std::pair<std::error_code, std::size_t>>(
      [&](auto cb) {
        connection.async_recv(buffers, cb);
      },
      connection);
  co_return result;
}

template <typename PortSpace, typename ConstBufferSequence>
inline async_simple::coro::Lazy<std::pair<std::error_code, std::size_t>>
async_send(nd_connection<PortSpace>& connection,
           ConstBufferSequence const& buffers) {
  if (!connection.has_executor()) {
    co_return std::make_pair(make_error_code(nd_errc::ext_no_executor),
                             std::size_t{0});
  }

  auto result = co_await async_io<std::pair<std::error_code, std::size_t>>(
      [&](auto cb) {
        connection.async_send(buffers, cb);
      },
      connection);
  co_return result;
}

template <typename PortSpace, typename MutableBufferSequence>
inline async_simple::coro::Lazy<std::pair<std::error_code, std::size_t>>
async_read(nd_connection<PortSpace>& connection,
           MutableBufferSequence const& buffers,
           nd_remote_addr_t const& remote_addr) {
  if (!connection.has_executor()) {
    co_return std::make_pair(make_error_code(nd_errc::ext_no_executor),
                             std::size_t{0});
  }

  auto result = co_await async_io<std::pair<std::error_code, std::size_t>>(
      [&](auto cb) {
        connection.async_read(buffers, remote_addr, cb);
      },
      connection);
  co_return result;
}

template <typename PortSpace, typename ConstBufferSequence>
inline async_simple::coro::Lazy<std::pair<std::error_code, std::size_t>>
async_write(nd_connection<PortSpace>& connection,
            ConstBufferSequence const& buffers,
            nd_remote_addr_t const& remote_addr) {
  if (!connection.has_executor()) {
    co_return std::make_pair(make_error_code(nd_errc::ext_no_executor),
                             std::size_t{0});
  }

  auto result = co_await async_io<std::pair<std::error_code, std::size_t>>(
      [&](auto cb) {
        connection.async_write(buffers, remote_addr, cb);
      },
      connection);
  co_return result;
}

}
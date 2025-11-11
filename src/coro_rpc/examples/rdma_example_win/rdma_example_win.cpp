/*
 * Copyright (c) 2025, Alibaba Group Holding Limited;
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <memory>
#include <thread>
#include "ylt/coro_io/networkdirect/nd_io.hpp"

void compile_check() {
  static_assert(coro_io::mr_buffer_sequence<
                std::array<coro_io::nd_mr_t::const_buffer, 4>>);
  static_assert(
      coro_io::mr_adapted_buffer_sequence<coro_io::nd_mr_t::const_buffer>);
  static_assert(coro_io::const_mr_buffer<coro_io::nd_mr_t::const_buffer>);
  static_assert(coro_io::mutable_mr_buffer<coro_io::nd_mr_t::mutable_buffer>);
  static_assert(
      coro_io::mr_const_buffer_sequence<coro_io::nd_mr_t::const_buffer>);
  static_assert(coro_io::mr_const_buffer_sequence<
                std::array<coro_io::nd_mr_t::const_buffer, 4>>);
  static_assert(
      coro_io::mr_mutable_buffer_sequence<coro_io::nd_mr_t::mutable_buffer>);
  static_assert(coro_io::mr_mutable_buffer_sequence<
                std::array<coro_io::nd_mr_t::mutable_buffer, 4>>);
}

void check_async_interface()
{
  // get device
  auto const& device_manager =
    coro_io::nd_device_manager_t::instance();
  auto device = device_manager.get_device(0);

  // create rdma connection
  using connection_t = coro_io::nd_connection<asio::ip::tcp>;
  connection_t connection{device};
  connection.open();

  // set io executor
  asio::io_context ioc{};
  //connection.set_executor(ioc.get_executor());
  connection.set_execution_context(ioc);

  // async connect
  asio::ip::tcp::endpoint endpoint{};
  connection.async_connect(endpoint, [](asio::error_code const& ec) {});

  // memory region
  std::array<int, 64> buffer{};
  // const buffer from memory region
  coro_io::nd_mr_t memory_region{device, buffer.data(), buffer.size()};
  auto const const_buffer = memory_region.cslice(buffer.data(), buffer.size());
  // test async send
  connection.async_send(const_buffer, [](asio::error_code const& ec,
                                         std::size_t bytes_transfered) {
  });
  // mutable buffer from memory region
  auto const mutable_buffer = memory_region.slice(buffer.data(), buffer.size());
  connection.async_recv(mutable_buffer, [](asio::error_code const& ec,
                                           std::size_t bytes_transfered) {
  });
  // fake remote address
  coro_io::nd_remote_addr_t remote_addr{};
  // test async write
  connection.async_write(
      const_buffer, remote_addr,
      [](asio::error_code const& ec, std::size_t bytes_transfered) {
  });
  connection.async_read(
      mutable_buffer, remote_addr,
      [](asio::error_code const& ec, std::size_t bytes_transfered) {
  });
}

static async_simple::coro::Lazy<int> check_coro_interface() {
  // get device
  auto const& device_manager = coro_io::nd_device_manager_t::instance();
  auto device = device_manager.get_device(0);

  // create rdma connection
  using connection_t = coro_io::nd_connection<asio::ip::tcp>;
  connection_t connection{device};
  connection.open();

  // set io executor
  asio::io_context ioc{};
  // connection.set_executor(ioc.get_executor());
  connection.set_execution_context(ioc);

  // forward
  asio::error_code ec{};
  std::size_t bytes_transfered = 0;

  // async connect
  asio::ip::tcp::endpoint endpoint{};
  ec = co_await async_connect(connection, endpoint);

  // memory region
  std::array<int, 64> buffer{};
  coro_io::nd_mr_t memory_region{device, buffer.data(), buffer.size()};
  // const buffer from memory region
  auto const const_buffer = memory_region.cslice(buffer.data(), buffer.size());
  // mutable buffer from memory region
  auto const mutable_buffer = memory_region.slice(buffer.data(), buffer.size());

  // async recv
  std::tie(ec, bytes_transfered) =
      co_await async_recv(connection, mutable_buffer);
  // async send
  std::tie(ec, bytes_transfered) =
      co_await async_send(connection, const_buffer);
  // fake remote address
  coro_io::nd_remote_addr_t remote_addr{};
  // async recv
  std::tie(ec, bytes_transfered) =
      co_await async_read(connection, mutable_buffer, remote_addr);
  // async send
  std::tie(ec, bytes_transfered) =
      co_await async_write(connection, const_buffer, remote_addr);
  co_return 0;
}

int main() {

  auto const& device_manager = coro_io::nd_device_manager_t::instance();
  auto device = device_manager.get_device(0);

  return 0;
}
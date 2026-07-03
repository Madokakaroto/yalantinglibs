/*
 * Copyright (c) 2026, Alibaba Group Holding Limited;
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
#include <chrono>
#include <cstdint>
#include <exception>
#include <string>
#include <system_error>
#include <thread>

#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>
#include <async_simple/coro/SyncAwait.h>
#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "doctest.h"
#include "rpc_api.hpp"
#include "ylt/coro_io/networkdirect/nd_socket.hpp"
#include "ylt/coro_rpc/impl/default_config/coro_rpc_config.hpp"

using namespace async_simple::coro;

namespace {

std::uint16_t pick_free_tcp_port() {
  asio::io_context io_ctx;
  asio::ip::tcp::acceptor acceptor(io_ctx);
  acceptor.open(asio::ip::tcp::v4());
  acceptor.bind({asio::ip::tcp::v4(), 0});
  auto port = acceptor.local_endpoint().port();
  acceptor.close();
  return port;
}

coro_io::nd_device_ptr try_get_nd_device() {
  try {
    return coro_io::nd_device_manager_t::instance().get_first_available_device(
        {});
  } catch (const std::exception& e) {
    MESSAGE("no ND device available, skipping RPC ND test: " << e.what());
    return nullptr;
  }
}

}  // namespace

TEST_CASE("coro_rpc over NetworkDirect") {
  auto device = try_get_nd_device();
  if (!device) {
    return;
  }

  std::string nd_host;
  try {
    nd_host = device->get_v4_address().to_string();
  } catch (const std::exception& e) {
    MESSAGE("ND device has no IPv4 address, skipping RPC ND test: "
            << e.what());
    return;
  }

  auto tcp_port = pick_free_tcp_port();
  auto nd_port = pick_free_tcp_port();

  coro_rpc::config_t server_config;
  server_config.thread_num = 2;
  server_config.address = "127.0.0.1";
  server_config.port = tcp_port;
  server_config.nd_address = nd_host;
  server_config.nd_port = nd_port;
  server_config.nd_config = coro_io::nd_socket_t::config_t{.device = device};

  coro_rpc::coro_rpc_server server(server_config);
  server.register_handler<hello, large_arg_fun, echo_with_attachment>();
  auto res = server.async_start();
  REQUIRE_MESSAGE(!res.hasResult(), "server start failed");
  REQUIRE(server.nd_port() == nd_port);

  coro_rpc::coro_rpc_client client(coro_io::get_global_executor());
  coro_io::nd_socket_t::config_t client_config{.device = device};
  REQUIRE(client.init_nd(client_config));

  auto ec = syncAwait(client.connect(nd_host, std::to_string(server.nd_port())));
  REQUIRE_MESSAGE(!ec, ec.message());

  auto hello_ret = client.sync_call<hello>();
  REQUIRE_MESSAGE(hello_ret.has_value(), "hello call failed");
  CHECK(hello_ret.value() == "hello");

  std::string large_arg(512 * 1024, 'n');
  auto large_ret = client.sync_call<large_arg_fun>(large_arg);
  REQUIRE_MESSAGE(large_ret.has_value(), "large_arg_fun call failed");
  CHECK(large_ret.value().size() == large_arg.size());
  CHECK(large_ret.value().front() == 'n');
  CHECK(large_ret.value().back() == 'n');

  client.set_req_attachment("nd attachment");
  auto attachment_ret = client.sync_call<echo_with_attachment>();
  REQUIRE_MESSAGE(attachment_ret.has_value(), "echo_with_attachment failed");
  CHECK(client.get_resp_attachment() == "nd attachment");

  client.close();
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  server.stop();
}

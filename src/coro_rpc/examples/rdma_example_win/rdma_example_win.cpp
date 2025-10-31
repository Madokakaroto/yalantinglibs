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
#include "ylt/coro_io/networkdirect/nd_connector.hpp"
#include "ylt/coro_io/networkdirect/nd_mr.hpp"

int main() {

  static_assert(
    coro_io::mr_buffer_sequence<std::array<coro_io::nd_mr_t::const_buffer, 4>>);
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

  auto const& device_manager =
    coro_io::nd_device_manager_t::instance();
  return 0;
}
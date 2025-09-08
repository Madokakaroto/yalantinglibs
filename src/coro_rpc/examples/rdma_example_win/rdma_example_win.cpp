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
#include "ylt/coro_io/networkdirect/nd_error.hpp"
#include "ylt/coro_io/networkdirect/nd_adapter.hpp"
#include "ylt/coro_io/networkdirect/detail/nd_op_base.hpp"

int main() {
  auto const& adapter_manager =
    coro_io::nd_adapter_manager_t::instance();
  return 0;
}
#pragma once

#include <ranges>
#include "ylt/coro_io/networkdirect/detail/nd_asio_manual_init.hpp"
#include "ylt/coro_io/networkdirect/nd_types.hpp"
#include "ylt/coro_io/networkdirect/nd_error.hpp"
#include "ylt/coro_io/networkdirect/detail/nd_device_impl.hpp"

namespace coro_io {

class nd_device_t {
  friend class nd_device_manager_t;

 private:
  detail::nd_provider_ptr provider_;
  detail::nd_adapter_ptr adapter_;

 protected:
  nd_device_t(detail::nd_provider_ptr const& provider,
              detail::nd_adapter_ptr const& adapter)
      : provider_(provider), adapter_(adapter) {
    if (!provider || !adapter) {
      asio::detail::throw_error(nd_errc::ext_invalid_device);
    }
  }


public:
  auto* get_pd() const {
    assert(adapter_);
    assert(adapter_->pd_);
    return adapter_->pd_.get();
  }

  std::string const& get_name() const {
    assert(adapter_);
    return adapter_->name_;
  }

  auto const& get_adapter() const {
    assert(adapter_);
    return adapter_;
  }
};
using nd_device_ptr = std::shared_ptr<nd_device_t>;

class nd_device_manager_t {
 private:
  detail::nd_global_t global_;
  std::vector<detail::nd_provider_ptr> providers_;

  nd_device_manager_t()
    : global_()
    , providers_(detail::get_providers()) {
    detail::open_adapters(providers_);
  }

 public:
  static nd_device_manager_t const& instance() {
    static nd_device_manager_t instance{};
    return instance;
  }

  //bool has_valid_adapter() const { return devices_.empty(); }
  //
  //size_t get_device_count() const { return devices_.size(); }
  nd_device_ptr get_device(size_t index) const {
    //if (index < devices_.size()) {
    //  return devices_.at(index);
    //}
    return nullptr;
  }

  // query adapter
  nd_device_ptr query_device(std::string const& adapter_name) const {
    //auto itr = std::ranges::find_if(devices_, [&](auto const& adapter) {
    //  return adapter->name_ == adapter_name;
    //  });
    //if (itr != devices_.end()) {
    //  return *itr;
    //}
    return nullptr;
  }

  nd_device_ptr query_device(nd_context_config_t const& config) {
    //auto valid_devices = devices_ 
    //  | std::views::filter([&](auto const& device) {
    //      return detail::is_valid_device(device, config);
    //    })
    //  | std::views::take(1);
    //if (std::ranges::empty(valid_devices)) {
    //  return nullptr;
    //}
    //return *std::ranges::begin(valid_devices);
      return nullptr;
  }

  nd_device_ptr query_device(std::string const& device_name,
                             nd_context_config_t const& config) {
    //auto valid_devices = devices_
    //  | std::views::filter([&](auto const& device) {
    //      if (!detail::is_valid_device(device, config)) {
    //        return false;
    //      }
    //      if (device_name.empty()) {
    //        return true;
    //      }
    //      return device->name_ == device_name;
    //    })
    //  | std::views::take(1);
    //if (std::ranges::empty(valid_devices)) {
    //  return nullptr;
    //}
    //return *std::ranges::begin(valid_devices);
    return nullptr;
  }
};

}
#pragma once

#include "ylt/coro_io/networkdirect/detail/nd_service_listener.hpp"

namespace coro_io {

template <typename PortSpace,
          typename Executor = asio::io_context::executor_type>
class nd_listener {
 public:
  using service_type = detail::nd_iocp_listener_service<PortSpace>;
  using executor_type = Executor;
  using port_space_type = PortSpace;
  using endpoint_type = typename port_space_type::endpoint;
  using config_t = typename service_type::config_t;

  /// Rebinds the connection type to another executor.
  template <typename Executor1>
  struct rebind_executor {
    /// The conection type when rebound to the specified executor.
    using other = nd_listener<PortSpace, Executor1>;
  };

  // All connections have access to each other's implementations.
  template <typename Protocol1, typename Executor1>
  friend class nd_listener;

 protected:
  // device that creates this connection
  nd_device_ptr device_;

  // shared state
  detail::nd_listener_state_ptr state_;

  // asio io object to call service interface
  using impl_type = asio::detail::io_object_impl<service_type>;
  std::unique_ptr<impl_type> pimpl_;

 public:
  ~nd_listener() = default;
  nd_listener(nd_listener const&) = delete;
  nd_listener& operator=(nd_listener const&) = delete;
  nd_listener(nd_listener&&) = default;
  nd_listener& operator=(nd_listener&&) = default;

  explicit nd_listener(nd_device_ptr const& device)
    : device_(device) {
    if (!device) {
      asio::detail::throw_error(nd_errc::ext_invalid_device);
    }
  }

  template <typename PortSpace1, typename Executor1>
    requires(asio::is_convertible<PortSpace1, PortSpace>::value &&
             asio::is_convertible<Executor1, Executor>::value)
  nd_listener(nd_listener<PortSpace1, Executor1>&& other)
      : device_(std::move(other.device_)), state_(std::move(other.state_)) {
    if (other.pimpl_) {
      pimpl_ = std::make_unique<impl_type>(*other.pimpl_);
    }
  }

  template <typename PortSpace1, typename Executor1>
    requires(asio::is_convertible<PortSpace1, PortSpace>::value &&
             asio::is_convertible<Executor1, Executor>::value)
  nd_listener& operator=(nd_listener<PortSpace1, Executor1>&& other) {
    nd_listener temp{std::move(other)};
    return *this = std::move(temp);
  }

 public:
  bool is_open() const noexcept { 
    return state_ != nullptr;
  }

  void open(config_t config = config_t{}) {
    asio::error_code ec{};
    open(config, ec);
    asio::detail::throw_error(ec, "open");
  }

  void open(config_t config, asio::error_code& ec) {
    if (!device_) {
      ec = nd_errc::ext_invalid_device;
      ASIO_ERROR_LOCATION(ec);
      return;
    }
    if (is_open()) {
      ec = asio::error::already_open;
      ASIO_ERROR_LOCATION(ec);
      return;
    }
    auto state = detail::create_listener_state(device_, config, ec);
    if (ec) {
      return;
    }
    assert(state);
    state_ = std::move(state);
    ec.clear();
  }

  bool has_executor() const noexcept {
    return pimpl_ != nullptr;
  }

  executor_type const& get_executor() const {
    return pimpl_->get_executor();
  }

  void set_executor(executor_type const& io_ex, asio::error_code& ec) {
    if (has_executor()) {
      ec = nd_errc::ext_already_registered;
      ASIO_ERROR_LOCATION(ec);
      return;
    }
    if (!state_) {
      ec = nd_errc::ext_invalid_connector;
      ASIO_ERROR_LOCATION(ec);
      return;
    }
    pimpl_ = std::make_unique<impl_type>(0, io_ex);
    pimpl_->get_service().register_state(pimpl_->get_implementation(), state_,
                                         ec);
  }

  void set_executor(executor_type const& io_ex) {
    asio::error_code ec{};
    set_executor(io_ex, ec);
    asio::detail::throw_error(ec, "set_executor");
  }

  template <typename ExecutionContext>
    requires(asio::is_convertible<ExecutionContext&,
                                  asio::execution_context&>::value)
  void set_execution_context(ExecutionContext& context, asio::error_code& ec) {
    if (has_executor()) {
      ec = nd_errc::ext_already_registered;
      ASIO_ERROR_LOCATION(ec);
      return;
    }
    if (!state_) {
      ec = nd_errc::ext_invalid_connector;
      ASIO_ERROR_LOCATION(ec);
      return;
    }
    pimpl_ = std::make_unique<impl_type>(0, 0, context);
    pimpl_->get_service().register_state(pimpl_->get_implementation(), state_,
                                         ec);
  }

  template <typename ExecutionContext>
    requires(asio::is_convertible<ExecutionContext&,
                                  asio::execution_context&>::value)
  void set_execution_context(ExecutionContext& context) {
    asio::error_code ec{};
    set_execution_context(context, ec);
    asio::detail::throw_error(ec, "set_execution_context");
  }

  void bind_addr(endpoint_type const& endpoint) {
    asio::error_code ec{};
    bind_addr(endpoint, ec);
    asio::detail::throw_error(ec, "bind_addr");
  }

  void bind_addr(endpoint_type const& endpoint, asio::error_code& ec) {
    if (!has_executor()) {
      ec = nd_errc::ext_no_executor;
      ASIO_ERROR_LOCATION(ec);
      return;
    }
    pimpl_->get_service().bind_addr(pimpl_->get_implementation(), endpoint, ec);
  }

  void listen(int backlog) {
    asio::error_code ec{};
    listen(backlog, ec);
    asio::detail::throw_error(ec, "listen");
  }

  void listen(int backlog, asio::error_code& ec) {
    if (!has_executor()) {
      ec = nd_errc::ext_no_executor;
      ASIO_ERROR_LOCATION(ec);
      return;
    }
    pimpl_->get_service().listen(pimpl_->get_implementation(), backlog, ec);
  }
};

}
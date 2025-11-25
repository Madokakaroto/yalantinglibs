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
  nd_listener() = default;
  ~nd_listener() = default;
  nd_listener(nd_listener const&) = delete;
  nd_listener& operator=(nd_listener const&) = delete;
  nd_listener(nd_listener&&) = default;
  nd_listener& operator=(nd_listener&&) = default;

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

  nd_listener(nd_device_ptr const& device,
              config_t const& config = config_t{})
    : device_(device) {
    // construct with open
    asio::error_code ec{};
    service_type::open(device, config, state_, ec);
    asio::detail::throw_error(ec, "open");
  }

  nd_listener(nd_device_ptr const& device, config_t const& config,
              int port_number)
    : device_(device) {
    // construct with open, bind & listen
    asio::error_code ec{};
    service_type::open(device, config, state_, ec);
    asio::detail::throw_error(ec, "open");

    service_type::bind(state_, port_number, ec);
    asio::detail::throw_error(ec, "bind");

    service_type::listen(state_, ec);
    asio::detail::throw_error(ec, "listen");
  }

 public:
  bool is_open() const noexcept { 
    return service_type::is_open(state_);
  }

  void open(nd_device_ptr const& device, config_t const& config = config_t{}) {
    asio::error_code ec{};
    open(device, config, ec);
    asio::detail::throw_error(ec, "open");
  }

  void open(nd_device_ptr const& device, config_t const& config,
            asio::error_code& ec) {
    detail::nd_listener_state_ptr state{};
    service_type::open(device, config, state, ec);
    if (!ec) {
      device_ = device;
      state_ = std::move(state);
    }
  }

  void bind(uint16_t port_number) {
    asio::error_code ec{};
    bind(port_number, ec);
    asio::detail::throw_error(ec, "bind");
  }

  void bind(uint16_t port_number, asio::error_code& ec) {
    service_type::bind(state_, port_number, ec);
  }

  void listen() {
    asio::error_code ec{};
    listen(ec);
    asio::detail::throw_error(ec, "listen");
  }

  void listen(asio::error_code& ec) {
    service_type::listen(state_, ec);
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

  void cancel() {
    // TODO ...
    assert(false);
  }

  // begin implement async write
 private:
  class initiate_async_accept {
   public:
    using executor_type = Executor;

    explicit initiate_async_accept(nd_listener* self)
        : self_(self) {}

    executor_type get_executor() const ASIO_NOEXCEPT {
      return self_->get_executor();
    }

    template <typename AcceptHandler, typename PortSpace1, typename Executor1>
    void operator()(ASIO_MOVE_ARG(AcceptHandler) handler,
                    nd_connection<PortSpace1, Executor1>& peer,
                    nd_config_t const& config) const {
      // If you get an error on the following line it means that your handler
      // does not meet the documented type requirements for a AcceptHandler.
      ASIO_ACCEPT_HANDLER_CHECK(AcceptHandler, handler) type_check;

      asio::detail::non_const_lvalue<AcceptHandler> handler2(handler);
      self_->pimpl_->get_service().async_accept(
          self_->pimpl_->get_implementation(),  // io object implementation
          peer, config,                       // peer connection & confit to initialize peer
          handler2.value,                     // handler
          self_->pimpl_->get_executor());  // io executor
    }

   private:
    nd_listener* self_;
  };

 public:
  template <typename Executor1,
            ASIO_COMPLETION_TOKEN_FOR(void(asio::error_code))
                AcceptToken ASIO_DEFAULT_COMPLETION_TOKEN_TYPE(executor_type)>
  ASIO_INITFN_AUTO_RESULT_TYPE_PREFIX(AcceptToken, void(asio::error_code))
  async_accept(nd_connection<port_space_type, Executor1>& peer,
               nd_config_t const& config,
               ASIO_MOVE_ARG(AcceptToken)
                   token ASIO_DEFAULT_COMPLETION_TOKEN(executor_type))
      ASIO_INITFN_AUTO_RESULT_TYPE_SUFFIX(
          (async_initiate<AcceptToken, void(asio::error_code)>(
              declval<initiate_async_accept>(), token, peer, config))) {
    return async_initiate<AcceptToken, void(asio::error_code)>(
        initiate_async_accept(this), token, peer, config);
  }
   // end implement async write

};

}
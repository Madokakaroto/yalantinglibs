#pragma once

#include "ylt/coro_io/networkdirect/detail/nd_service_connector.hpp"

namespace coro_io {

template <typename PortSpace,
          typename Executor = asio::io_context::executor_type>
class nd_connection {
public:
  using service_type = detail::nd_iocp_connector_service<PortSpace>;
  using executor_type = Executor;
  using port_space_type = PortSpace;
  using endpoint_type = typename port_space_type::endpoint;
  using config_t = typename service_type::config_t;

  /// Rebinds the connection type to another executor.
  template <typename Executor1>
  struct rebind_executor {
    /// The conection type when rebound to the specified executor.
    using other = nd_connection<PortSpace, Executor1>;
  };

  // All connections have access to each other's implementations.
  template <typename Protocol1, typename Executor1>
  friend class nd_connection;

protected:
  // device that creates this connection
  nd_device_ptr device_;

  // shared state
  detail::nd_connector_state_ptr state_;

  // asio io object to call service interface
  using impl_type = asio::detail::io_object_impl<service_type>;
  std::unique_ptr<impl_type> pimpl_;

public:
  nd_connection() = default;
  ~nd_connection() = default;
  nd_connection(nd_connection const&) = delete;
  nd_connection& operator=(nd_connection const&) = delete;
  nd_connection(nd_connection&&) = default;
  nd_connection& operator=(nd_connection&&) = default;

  template <typename PortSpace1, typename Executor1>
    requires(asio::is_convertible<PortSpace1, PortSpace>::value &&
             asio::is_convertible<Executor1, Executor>::value)
  nd_connection(nd_connection<PortSpace1, Executor1>&& other)
    : device_(std::move(other.device_))
    , state_(std::move(other.state_)){
    if (other.pimpl_) {
      pimpl_ = std::make_unique<impl_type>(*other.pimpl_);
    }
  }

  template <typename PortSpace1, typename Executor1>
   requires(asio::is_convertible<PortSpace1, PortSpace>::value &&
            asio::is_convertible<Executor1, Executor>::value)
  nd_connection& operator=(nd_connection<PortSpace1, Executor1>&& other) {
    nd_connection temp{std::move(other)};
    return *this = std::move(temp);
  }

  nd_connection(nd_device_ptr const& device,
                config_t const& config = config_t{})
      : device_(device) {
    // construct with open
    asio::error_code ec{};
    service_type::open(device, config, state_, ec);
    asio::detail::throw_error(ec, "open");
  }

public:
  bool is_open() const noexcept {
    return service_type::is_open(state_);
  }

  void open(nd_device_ptr const& device, config_t config = config_t{}) {
    asio::error_code ec{};
    open(device, config, ec);
    asio::detail::throw_error(ec, "open");
  }

  void open(nd_device_ptr const& device, config_t config,
            asio::error_code& ec) {
    detail::nd_connector_state_ptr state{};
    service_type::open(device, config, state, ec);
    if (!ec) {
      device_ = device;
      state_ = std::move(state);
    }
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
    requires(asio::is_convertible<ExecutionContext&, asio::execution_context&>::value)
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
    requires(asio::is_convertible<ExecutionContext&, asio::execution_context&>::value)
  void set_execution_context(ExecutionContext& context)
  {
    asio::error_code ec{};
    set_execution_context(context, ec);
    asio::detail::throw_error(ec, "set_execution_context");
  }

  void cancel() {
    // TODO ...
    assert(false);
  }

  void assign(nd_device_ptr const& device,
              detail::nd_connector_state_ptr const& state,
              asio::error_code& ec) {
    if (is_open()) {
      ec = nd_errc::ext_invalid_device;
      ASIO_ERROR_LOCATION(ec);
      return;
    }
    if (has_executor()) {
      ec = nd_errc::ext_already_registered;
      ASIO_ERROR_LOCATION(ec);
      return;
    }
    if (!device) {
      ec = nd_errc::ext_invalid_device;
      ASIO_ERROR_LOCATION(ec);
      return;
    }
    device_ = device;
    state_ = state;
  }

  void assign(nd_device_ptr const& device,
              detail::nd_connector_state_ptr const& state) {
    asio::error_code ec{};
    assign(device, state, ec);
    asio::detail::throw_error(ec, "assign");
  }

  // begin implement async connect
 private: 
  class initiate_async_connect {
   private:
    nd_connection* self_;

   public:
    explicit initiate_async_connect(nd_connection* self) noexcept
        : self_(self) {}

    template <typename Handler>
    void operator()(ASIO_MOVE_ARG(Handler) handler,
                    endpoint_type const& endpoint,
                    asio::error_code const& open_ec) const {
      ASIO_CONNECT_HANDLER_CHECK(Handler, handler) type_check;

      if (open_ec) {
        asio::post(self_->pimpl_->get_executor(),
                   asio::detail::bind_handler(ASIO_MOVE_CAST(Handler)(handler),
                                              open_ec));
      }
      else {
        asio::detail::non_const_lvalue<Handler> handler2(handler);
        self_->pimpl_->get_service().async_connect(
            self_->pimpl_->get_implementation(), endpoint, handler2.value,
            self_->pimpl_->get_executor());
      }
    }
  };

public:
  template <ASIO_COMPLETION_TOKEN_FOR(void(asio::error_code))
                ConnectToken ASIO_DEFAULT_COMPLETION_TOKEN_TYPE(executor_type)>
  ASIO_INITFN_AUTO_RESULT_TYPE_PREFIX(ConnectToken, void(asio::error_code))
  async_connect(endpoint_type const& endpoint,
                ASIO_MOVE_ARG(ConnectToken) token
                ASIO_DEFAULT_COMPLETION_TOKEN(executor_type))
      ASIO_INITFN_AUTO_RESULT_TYPE_SUFFIX(
          (async_initiate<ConnectToken, void(asio::error_code)>(
              declval<initiate_async_connect>(), token,
              declval<asio::error_code&>()))) {
    asio::error_code open_ec{};
    if (!is_open()) {
      open_ec = asio::error::try_again;
    }
    return asio::async_initiate<ConnectToken, void(asio::error_code)>(
        initiate_async_connect(this), token, endpoint, open_ec);
  }
  // end implement async connect

  // begin implement async send
 private:
  class initiate_async_send {
   private:
    nd_connection* self_;

   public:
    explicit initiate_async_send(nd_connection* self) : self_(self) {}
    template <typename WriteHandler, typename ConstBufferSequence>
    void operator()(ASIO_MOVE_ARG(WriteHandler) handler,
                    const ConstBufferSequence& buffers) const {
      // If you get an error on the following line it means that your handler
      // does not meet the documented type requirements for a WriteHandler.
      ASIO_WRITE_HANDLER_CHECK(WriteHandler, handler) type_check;

      asio::detail::non_const_lvalue<WriteHandler> handler2(handler);
      self_->pimpl_->get_service().async_send(
          self_->pimpl_->get_implementation(), buffers, handler2.value,
          self_->pimpl_->get_executor());
    }
  };

 public:
  template <mr_const_buffer_sequence ConstBufferSequence,
            ASIO_COMPLETION_TOKEN_FOR(void(asio::error_code, std::size_t))
                WriteToken ASIO_DEFAULT_COMPLETION_TOKEN_TYPE(executor_type)>
  ASIO_INITFN_AUTO_RESULT_TYPE_PREFIX(WriteToken,
                                      void(asio::error_code, std::size_t))
  async_send(const ConstBufferSequence& buffers,
             ASIO_MOVE_ARG(WriteToken)
                 token ASIO_DEFAULT_COMPLETION_TOKEN(executor_type))
      ASIO_INITFN_AUTO_RESULT_TYPE_SUFFIX((
          asio::async_initiate<WriteToken, void(asio::error_code, std::size_t)>(
              initiate_async_send(this), token, buffers))) {
    return asio::async_initiate<WriteToken,
                                void(asio::error_code, std::size_t)>(
        initiate_async_send(this), token, buffers);
  }
  // end implement async send

  // begin implement async recv
 private:
  class initiate_async_recv {
   private:
    nd_connection* self_;

   public:
    explicit initiate_async_recv(nd_connection* self) : self_(self) {}
    template <typename ReadHandler, typename MutableBufferSequence>
    void operator()(ASIO_MOVE_ARG(ReadHandler) handler,
                    const MutableBufferSequence& buffers) const {
      // If you get an error on the following line it means that your handler
      // does not meet the documented type requirements for a ReadHandler.
      ASIO_READ_HANDLER_CHECK(ReadHandler, handler) type_check;

      asio::detail::non_const_lvalue<ReadHandler> handler2(handler);
      self_->pimpl_->get_service().async_recv(
          self_->pimpl_->get_implementation(), buffers, handler2.value,
          self_->pimpl_->get_executor());
    }
  };

 public:
  template <typename MutableBufferSequence,
            ASIO_COMPLETION_TOKEN_FOR(void(asio::error_code, std::size_t))
                ReadToken ASIO_DEFAULT_COMPLETION_TOKEN_TYPE(executor_type)>
  ASIO_INITFN_AUTO_RESULT_TYPE_PREFIX(ReadToken,
                                      void(asio::error_code, std::size_t))
  async_recv(const MutableBufferSequence& buffers,
             ASIO_MOVE_ARG(ReadToken)
                 token ASIO_DEFAULT_COMPLETION_TOKEN(executor_type))
      ASIO_INITFN_AUTO_RESULT_TYPE_SUFFIX(
          (asio::async_initiate<ReadToken, void(asio::error_code, std::size_t)>(
              declval<initiate_async_recv>(), token, buffers))) {
    return asio::async_initiate<ReadToken, void(asio::error_code, std::size_t)>(
        initiate_async_recv(this), token, buffers);
  }
  // end implement async recv

  // begin implement async write
 private:
  class initiate_async_write {
   private:
    nd_connection* self_;

   public:
    explicit initiate_async_write(nd_connection* self) : self_(self) {}
    template <typename WriteHandler, typename ConstBufferSequence>
    void operator()(ASIO_MOVE_ARG(WriteHandler) handler,
                    const ConstBufferSequence& buffers,
                    nd_remote_addr_t const& remote_addr) const {
      // If you get an error on the following line it means that your handler
      // does not meet the documented type requirements for a WriteHandler.
      ASIO_WRITE_HANDLER_CHECK(WriteHandler, handler) type_check;

      asio::detail::non_const_lvalue<WriteHandler> handler2(handler);
      self_->pimpl_->get_service().async_write(
          self_->pimpl_->get_implementation(), buffers, remote_addr,
          handler2.value, self_->pimpl_->get_executor());
    }
  };

 public:
  template <typename ConstBufferSequence,
            ASIO_COMPLETION_TOKEN_FOR(void(asio::error_code, std::size_t))
                WriteToken ASIO_DEFAULT_COMPLETION_TOKEN_TYPE(executor_type)>
  ASIO_INITFN_AUTO_RESULT_TYPE_PREFIX(WriteToken,
                                      void(asio::error_code, std::size_t))
  async_write(const ConstBufferSequence& buffers,
              nd_remote_addr_t const& remote_addr,
              ASIO_MOVE_ARG(WriteToken)
                  token ASIO_DEFAULT_COMPLETION_TOKEN(executor_type))
      ASIO_INITFN_AUTO_RESULT_TYPE_SUFFIX((
          asio::async_initiate<WriteToken, void(asio::error_code, std::size_t)>(
              initiate_async_write(this), token, buffers))) {
    return asio::async_initiate<WriteToken,
                                void(asio::error_code, std::size_t)>(
        initiate_async_write(this), token, buffers, remote_addr);
  }
  // end implement async write

  // begin implement async read
 private:
  class initiate_async_read {
   private:
    nd_connection* self_;

   public:
    explicit initiate_async_read(nd_connection* self) : self_(self) {}
    template <typename ReadHandler, typename MutableBufferSequence>
    void operator()(ASIO_MOVE_ARG(ReadHandler) handler,
                    const MutableBufferSequence& buffers,
                    nd_remote_addr_t const& remote_addr) const {
      // If you get an error on the following line it means that your handler
      // does not meet the documented type requirements for a ReadHandler.
      ASIO_READ_HANDLER_CHECK(ReadHandler, handler) type_check;

      asio::detail::non_const_lvalue<ReadHandler> handler2(handler);
      self_->pimpl_->get_service().async_read(
          self_->pimpl_->get_implementation(), buffers, remote_addr,
          handler2.value, self_->pimpl_->get_executor());
    }
  };

 public:
  template <typename MutableBufferSequence,
            ASIO_COMPLETION_TOKEN_FOR(void(asio::error_code, std::size_t))
                ReadToken ASIO_DEFAULT_COMPLETION_TOKEN_TYPE(executor_type)>
  ASIO_INITFN_AUTO_RESULT_TYPE_PREFIX(ReadToken,
                                      void(asio::error_code, std::size_t))
  async_read(const MutableBufferSequence& buffers,
             nd_remote_addr_t const& remote_addr,
             ASIO_MOVE_ARG(ReadToken)
                 token ASIO_DEFAULT_COMPLETION_TOKEN(executor_type))
      ASIO_INITFN_AUTO_RESULT_TYPE_SUFFIX(
          (asio::async_initiate<ReadToken, void(asio::error_code, std::size_t)>(
              declval<initiate_async_recv>(), token, buffers))) {
    return asio::async_initiate<ReadToken, void(asio::error_code, std::size_t)>(
        initiate_async_read(this), token, buffers, remote_addr);
  }
  // end implement async read

};


}
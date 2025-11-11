#pragma once

#include "asio/detail/cstdint.hpp"
#include "asio/detail/handler_alloc_helpers.hpp"
#include "asio/detail/memory.hpp"
#include "asio/detail/mutex.hpp"
#include "ylt/coro_io/networkdirect/detail/nd_service_base.hpp"
#include "ylt/coro_io/networkdirect/detail/nd_op_accept.hpp"

namespace coro_io::detail {

template <typename PortSpace>
class nd_iocp_listener_service
    : public asio::detail::execution_context_service_base<
          nd_iocp_listener_service<PortSpace>>,
      public nd_service_base {
public:
  /// export public types
  using base_type = asio::detail::execution_context_service_base<
     nd_iocp_connector_service<PortSpace>>;

  // the port space type
  using port_space_type = PortSpace;

  // the endpoint type
  using endpoint_type = typename port_space_type::endpoint;

  // configuration type to initialize the shared state
  using config_t = nd_config_t;

  // shared state of a rdma connection:
  using shared_state_t = nd_listener_state_t;
  using shared_state_ptr = nd_listener_state_ptr;

  // implementation_type used by asio::detail::io_object_impl
  struct implementation_type : nd_service_base::base_implementation_type {
    shared_state_ptr state_;
  };

protected:
  asio::error_code success_ec_;

public:
  explicit nd_iocp_listener_service(asio::execution_context& context)
      : base_type(context)
      , nd_service_base(context)
      , success_ec_() {
  }

  ~nd_iocp_listener_service() = default;

public: // implementation of execution_context::service
  virtual void shutdown() override {
    base_shutdown<implementation_type>([this](implementation_type& impl) {
      close_for_destruction(impl);
    });
  }

  virtual void notify_fork(asio::execution_context::fork_event event) override {
    // TODO ... notify fork
  }

public: // rule of five, used by asio::detail::io_object_impl
  void construct(implementation_type& impl) {
    nd_service_base::base_construct(impl);
    impl.state_.reset();
  }

  void destroy(implementation_type& impl) {
    close_for_destruction(impl);
    nd_service_base::base_destroy(impl);
  }

  void move_construct(implementation_type& impl,
                      implementation_type& other_impl) {
    nd_service_base::base_move_construct(impl, other_impl);
    impl.state_ = std::move(other_impl.state_);
  }

  void move_assign(implementation_type& impl,
                   nd_iocp_listener_service& other_service,
                   implementation_type& other_impl) {
    close_for_destruction(impl);
    nd_service_base::base_move_assign(impl, other_service, other_impl);
    if (this != &other_service) {
      this->remove(impl);
    }
    impl.state_ = std::move(other_impl.state_);
    if (this != &other_service) {
      other_service.insert(impl);
    }
  }

public: // public interfaces
  bool has_state(implementation_type const& impl) const {
    return impl.state_ != nullptr;
  }

  asio::error_code register_state(implementation_type& impl,
                                  shared_state_ptr const& shared_state,
                                  asio::error_code& ec) {
    if (has_state(impl)) {
      ec = nd_errc::ext_already_registered;
      ASIO_ERROR_LOCATION(ec);
      return ec;
    }
    if (!shared_state) {
      ec = nd_errc::ext_invalid_listener;
      ASIO_ERROR_LOCATION(ec);
      return ec;
    }
    this->scheduler_.register_handle(shared_state->overlapped_handle_.get(),
                                     ec);
    if (!ec) {
      impl.state_ = shared_state;
    }
    return ec;
  }

  void close(implementation_type& impl) {
    close_for_destruction(impl);
  }

  asio::error_code bind_addr(implementation_type& impl,
                             endpoint_type const& endpoint,
                             asio::error_code& ec) {
    if (!has_listener(impl)) {
      ec = nd_errc::ext_invalid_listener;
      ASIO_ERROR_LOCATION(ec);
      return ec;
    }
    detail::bind_addr(impl.state_->listener_.Get(), endpoint.data(),
                      endpoint.size(), ec);
    if (ec) {
      ASIO_ERROR_LOCATION(ec);
    }
    return ec;
  }

  asio::error_code listen(implementation_type& impl, int backlog,
                          asio::error_code& ec) {
    if (!has_listener(impl)) {
      ec = nd_errc::ext_invalid_listener;
      ASIO_ERROR_LOCATION(ec);
      return ec;
    }
    detail::listen(impl.state_->listener_.Get(), backlog, ec);
    if (ec) {
      ASIO_ERROR_LOCATION(ec);
    }
    return ec;
  }

private:
  void close_for_destruction(implementation_type& impl) {
    if (is_open(impl)) {
      impl.state_->listener_.Reset();
      impl.state_->handle_.reset();
      impl.state_->device_.reset();
    }
  }

  bool has_listener(implementation_type const& impl) const {
    return has_state(impl) && impl.state_->listener_ != nullptr;
  }

  void start_accept_op(implementation_type& impl,
                       nd_connector_state_ptr& connector_state,
                       nd_accept_op_base* op) {
    this->scheduler_.work_started();
    if (!has_listener(impl)) {
      this->scheduler_.on_completion(op, nd_errc::ext_invalid_listener);
    }
    else {

    }
  }
};

}
#pragma once

#include "asio/detail/buffer_sequence_adapter.hpp"
#include "asio/detail/cstdint.hpp"
#include "asio/detail/handler_alloc_helpers.hpp"
#include "asio/detail/memory.hpp"
#include "asio/detail/mutex.hpp"
#include "ylt/coro_io/networkdirect/detail/nd_service_base.hpp"
#include "ylt/coro_io/networkdirect/detail/nd_ops_verbs.hpp"
#include "ylt/coro_io/networkdirect/detail/nd_ops_cm.hpp"
#include "ylt/coro_io/networkdirect/detail/nd_op_connect.hpp"
#include "ylt/coro_io/networkdirect/detail/nd_op_accept.hpp"

namespace coro_io::detail {

template <typename PortSpace>
class nd_iocp_connector_service
  : public asio::detail::execution_context_service_base<
          nd_iocp_connector_service<PortSpace>>
  , public nd_service_base {
public:
  /// export public types
  using base_type = asio::detail::execution_context_service_base<
     nd_iocp_connector_service<PortSpace>>;

  // the port space type
  using port_space_type = PortSpace;

  // the endpoint type
  using endpoint_type = typename port_space_type::endpoint;

  // configuration type to initialize the shared state
  using config_t = nd_connector_config_t;

  // shared state of a rdma connection:
  using shared_state_t = nd_connector_state_t;
  using shared_state_ptr = nd_connector_state_ptr;

  // implementation_type used by asio::detail::io_object_imipl
  struct implementation_type : nd_service_base::base_implementation_type {
    shared_state_ptr state_;
  };

public:
  explicit nd_iocp_connector_service(asio::execution_context& context)
    : base_type(context)
    , nd_service_base(context) {

  }

  ~nd_iocp_connector_service() = default;

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
                   nd_iocp_connector_service& other_service,
                   implementation_type& other_impl) {
    close_for_destruction(impl);
    if (this != &other_service) {
      this->remove(impl);
    }
    impl.state_ = std::move(other_impl.state_);
    if (this != &other_service) {
      other_service.insert(impl);
    }
  }

public: // public interfaces on implementation type
  bool has_valid_state(implementation_type const& impl) const {
    return impl.state_ != nullptr;
  }

  asio::error_code set_state(implementation_type& impl,
                             shared_state_ptr const& shared_state,
                             asio::error_code& ec) const {
    if (is_open(impl)) {
      ec = asio::error::already_open;
      ASIO_ERROR_LOCATION(ec);
      return ec;
    }
    close_for_destruction(impl);
  }

  bool is_open(implementation_type const& impl) const { 
    return has_valid_state(impl) && impl.state_->is_opended;
  }

  asio::error_code open(implementation_type& impl,
                        shared_state_ptr const& shared_state,
                        asio::error_code& ec) {
    if (is_open(impl)) {
      ec = asio::error::already_open;
      ASIO_ERROR_LOCATION(ec);
      return ec;
    }
    do_open(shared_state, ec);
    if (ec) {
      return ec;
    }
    close_for_destruction(impl);
    impl.state_ = shared_state;
    return ec;
  }

  asio::error_code open(implementation_type& impl, asio::error_code& ec) {
    if (!has_valid_state(impl)) {
      ec = nd_errc::ndext_invalid_connector;
      ASIO_ERROR_LOCATION(ec);
      return ec;
    }
    if (is_open(impl)) {
      ec = asio::error::already_open;
      ASIO_ERROR_LOCATION(ec);
      return ec;
    }
    do_open(impl, ec);
    if (ec) {
      return ec;
    }
  }

  asio::error_code do_open(shared_state_ptr const& shared_state,
                           asio::error_code& ec) {
    register_state(shared_state, ec);
    if (ec) {
      ASIO_ERROR_LOCATION(ec);
      return ec;
    }
    ec.clear();
    return ec;
  }

  void close(implementation_type& impl) {
    close_for_destruction(impl);
  }
  
  asio::error_code bind_addr(implementation_type& impl,
                             sockaddr const* addrin, std::size_t addr_size,
                             asio::error_code& ec) {
    if (impl.state_ == nullptr || impl.state_->connector_) {
      ec = nd_errc::ndext_invalid_connector;
      ASIO_ERROR_LOCATION(ec);
      return ec;
    }

    bind_addr(impl.state_->connector_.Get(), addrin, addr_size, ec);
    if (ec) {
      ASIO_ERROR_LOCATION(ec);
    }
    return ec;
  }

 public: // public interfaces on shared state type
  asio::error_code register_state(shared_state_ptr& shared_state,
                                  asio::error_code& ec) {
    assert(shared_state->is_opened_ == false);
    this->scheduler_.register_handle(shared_state->overlapped_handle_.get(),
                                     ec);
    shared_state->is_opened_ = true;
    return ec;
  }

private:
  void close_for_destruction(implementation_type& impl) {
   if (has_valid_state(impl)) {
     // ASIO_HANDLER_OPERATION((context(), "handle", &impl,
     // reinterpret_cast<uintmax_t>(impl.connector_.Get()), "close"));
     impl.state_->qp_.Reset(); 
     impl.state_->cq_.Reset();
     impl.state_->connector_.Reset();
     impl.state_->overlapped_handle_.reset();
   }
  }

};

}
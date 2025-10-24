#pragma once

#include "asio/detail/buffer_sequence_adapter.hpp"
#include "asio/detail/cstdint.hpp"
#include "asio/detail/handler_alloc_helpers.hpp"
#include "asio/detail/memory.hpp"
#include "asio/detail/mutex.hpp"
#include "ylt/coro_io/networkdirect/detail/nd_service_base.hpp"
#include "ylt/coro_io/networkdirect/detail/nd_verbs_ops.hpp"

namespace coro_io::detail {

/* template <typename PortSpace> */
class nd_iocp_connector_service
  : public asio::detail::execution_context_service_base<
          nd_iocp_connector_service/*<PortSpace>*/>
  , public nd_service_base {
public:
  /// export public types
  using base_type = asio::detail::execution_context_service_base<
     nd_iocp_connector_service/*<PortSpace>*/>;

  // configuration type to initialize the shared state
  // TODO ... align with ibverbs
  struct config_t {
    size_type cqe_ = 64;
    size_type max_send_wr_ = 32;
    size_type max_recv_wr_ = 32;
    size_type max_send_sge_ = 8;
    size_type max_recv_sge_ = 8;
    size_type max_inline_data_ = 16;
  };

  // shared state of a rdma connection:
  struct shared_state_t {
    // overlapped handle to receive IO completion
    unique_handle_t overlapped_handle_;
    // the network-direect connector interface
    nd2_connector_ptr connector_;
    // the completion queue interface to poll IO work completion
    nd2_completion_queue_ptr cq_;
    // the queue pair interface to perform verbs IO operations
    nd2_queue_pair_ptr qp_;
  };
  using shared_state_ptr = std::shared_ptr<shared_state_t>;

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

public: // public interfaces
  bool is_open(implementation_type const& impl) { 
    return impl.state_ != nullptr && impl.state_->qp_ != nullptr; 
  }

  bool is_valid_device(nd_device_ptr const& device) {
    return device != nullptr && device->adapter_ != nullptr;
  }

  bool is_config_valid(nd_device_ptr const& device, config_t const& config) {
    assert(is_valid_device(device));
    return true;
  }

  shared_state_ptr create_shared_state(nd_device_ptr const& device,
                                       config_t const& config,
                                       asio::error_code& ec) {
    assert(is_valid_device(device));

    // create overlapped handle for notification of IO completion
    unique_handle_t overlapped_handle{};
    overlapped_handle.reset(
        create_overlapped_file(device->adapter_.Get(), ec));
    if (ec) {
      ASIO_ERROR_LOCATION(ec);
      return nullptr;
    }

    // create network-direct connector interface
    nd2_connector_ptr connector{};
    connector.Attach(
        create_connector(device->adapter_.Get(), overlapped_handle.get(), ec));
    if (ec) {
      ASIO_ERROR_LOCATION(ec);
      return nullptr;
    }

    // create verbs completion queue
    nd2_completion_queue_ptr cq{};
    native_cq_init_attr cq_init_attr{
        .overlapped_handle_ = overlapped_handle.get(),
        .processor_group_ = 0,
        .processor_affinity_ = 0,
    };
    cq.Attach(verbs_ops::create_cq(device->adapter_.Get(), config.cqe_,
                                   cq_init_attr, ec));
    if (ec) {
      ASIO_ERROR_LOCATION(ec);
      return nullptr;
    }

    // create verbs queue pair
    nd2_queue_pair_ptr qp{};
    native_qp_init_attr qp_init_attr{
        .qp_context_ = nullptr,
        .rcq_ = cq.Get(),
        .icq_ = cq.Get(),
        .max_send_wr_ = config.max_send_wr_,
        .max_recv_wr_ = config.max_recv_wr_,
        .max_send_sge_ = config.max_send_sge_,
        .max_recv_sge_ = config.max_recv_sge_,
        .max_inline_data_ = config.max_inline_data_,
    };
    qp.Attach(verbs_ops::create_qp(device->pd_.get(), qp_init_attr, ec));
    if (ec) {
      ASIO_ERROR_LOCATION(ec);
      return nullptr;
    }

    // exceptional-safty codes
    auto shared_state = std::make_shared<shared_state_t>();
    shared_state->overlapped_handle_ = std::move(overlapped_handle);
    shared_state->connector_ = std::move(connector);
    shared_state->cq_ = std::move(cq);
    shared_state->qp_ = std::move(qp);
    return shared_state;
  }

  void bind_shared_state(shared_state_ptr const& shared_state,
                         asio::error_code& ec) {
    this->scheduler_.register_handle(
      shared_state->overlapped_handle_.get(), ec);
  }

private:
  void close_for_destruction(implementation_type& impl) {
   if (is_open(impl)) {
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
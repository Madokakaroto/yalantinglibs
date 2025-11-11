#pragma once

#include "asio/detail/buffer_sequence_adapter.hpp"
#include "asio/detail/cstdint.hpp"
#include "asio/detail/handler_alloc_helpers.hpp"
#include "asio/detail/memory.hpp"
#include "asio/detail/mutex.hpp"
#include "ylt/coro_io/networkdirect/detail/nd_service_base.hpp"
#include "ylt/coro_io/networkdirect/detail/nd_ops_verbs.hpp"
#include "ylt/coro_io/networkdirect/detail/nd_ops_cm.hpp"
#include "ylt/coro_io/networkdirect/nd_mr.hpp"
#include "ylt/coro_io/networkdirect/nd_buffer.hpp"
#include "ylt/coro_io/networkdirect/detail/nd_op_notify_wr.hpp"
#include "ylt/coro_io/networkdirect/detail/nd_op_complete.hpp"
#include "ylt/coro_io/networkdirect/detail/nd_op_connect.hpp"
#include "ylt/coro_io/networkdirect/detail/nd_op_send.hpp"
#include "ylt/coro_io/networkdirect/detail/nd_op_recv.hpp"
#include "ylt/coro_io/networkdirect/detail/nd_op_write.hpp"
#include "ylt/coro_io/networkdirect/detail/nd_op_read.hpp"

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
  using config_t = nd_config_t;

  // shared state of a rdma connection:
  using shared_state_t = nd_connector_state_t;
  using shared_state_ptr = nd_connector_state_ptr;

  // implementation_type used by asio::detail::io_object_impl
  struct implementation_type : nd_service_base::base_implementation_type {
    shared_state_ptr state_;
  };

protected:
  asio::error_code success_ec_;

public:
  explicit nd_iocp_connector_service(asio::execution_context& context)
    : base_type(context)
    , nd_service_base(context)
    , success_ec_() {
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
      ec = nd_errc::ext_invalid_connector;
      ASIO_ERROR_LOCATION(ec);
      return ec;
    }
    this->scheduler_.register_handle(shared_state->overlapped_handle_.get(),
                                     ec);
    if (!ec)
    {
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
    if (impl.state_ == nullptr || impl.state_->connector_) {
      ec = nd_errc::ext_invalid_connector;
      ASIO_ERROR_LOCATION(ec);
      return ec;
    }

    detail::bind_addr(impl.state_->connector_.Get(), endpoint.data(),
                      endpoint.size(), ec);
    if (ec) {
      ASIO_ERROR_LOCATION(ec);
    }
    return ec;
  }

public: // async interfaces
  template <typename Handler, typename IoExecutor>
  void async_connect(implementation_type& impl, endpoint_type const& endpoint,
                     Handler& handler, IoExecutor const& io_ex) {
    // TODO ... cancellation
    using op = nd_connect_op<Handler, IoExecutor>;
    typename op::ptr p = {asio::detail::addressof(handler),
                          op::ptr::allocate(handler), 0};

    auto& connector = impl.state_->connector_;
    p.p = new (p.v) op{connector.Get(), handler, io_ex};

    // ASIO_HANDLER_CREATION((reactor_.context(), *p.p, "socket",
    //   &impl, impl.socket_, "async_accept"));

    start_connect_op(impl, endpoint, p.p);
    p.v = p.p = 0;
  }

  template <mr_mutable_buffer_sequence BufferSequence,
            typename Handler, typename IoExecutor>
  void async_recv(implementation_type& impl, BufferSequence const& buffers,
                  Handler& handler, IoExecutor const& io_ex) {
    typename asio::associated_cancellation_slot<Handler>::type slot =
        asio::get_associated_cancellation_slot(handler);

    // Allocate and construct an operation to wrap the handler.
    using op = nd_recv_op<BufferSequence, Handler, IoExecutor>;
    typename op::ptr p = {asio::detail::addressof(handler),
                          op::ptr::allocate(handler), 0};

    // construct operation
    p.p = new (p.v) op{success_ec_, buffers, handler, io_ex};

    // Optionally register for per-operation cancellation.
    if (slot.is_connected()) {
    }

    // TODO ...
    // ASIO_HANDLER_CREATION((this->context(), *p.p, "rdma", &impl, impl.cm_id_,
    // "async_recv"));

    start_recv_op(impl, p.p);
    p.v = p.p = 0;
  }

  template <mr_const_buffer_sequence BufferSequence, typename Handler,
            typename IoExecutor>
  void async_send(implementation_type& impl, BufferSequence const& buffers,
                  Handler& handler, IoExecutor const& io_ex) {
    typename asio::associated_cancellation_slot<Handler>::type slot =
        asio::get_associated_cancellation_slot(handler);

    // Allocate and construct an operation to wrap the handler.
    using op = nd_send_op<BufferSequence, Handler, IoExecutor>;
    typename op::ptr p = {asio::detail::addressof(handler),
                          op::ptr::allocate(handler), 0};

    // construct operation
    p.p = new (p.v) op{success_ec_, buffers, handler, io_ex};

    // Optionally register for per-operation cancellation.
    if (slot.is_connected()) {
    }

    // TODO ...
    // ASIO_HANDLER_CREATION((this->context(), *p.p, "rdma", &impl, impl.cm_id_,
    // "async_send"));

    start_send_op(impl, p.p);
    p.v = p.p = 0;
  }

  template <mr_mutable_buffer_sequence BufferSequence,
            typename Handler, typename IoExecutor>
  void async_read(implementation_type& impl, BufferSequence const& buffers,
                  nd_remote_addr_t const& remote_addr, Handler& handler,
                  IoExecutor const& io_ex) {
    typename asio::associated_cancellation_slot<Handler>::type slot =
        asio::get_associated_cancellation_slot(handler);

    // Allocate and construct an operation to wrap the handler.
    using op = nd_read_op<BufferSequence, Handler, IoExecutor>;
    typename op::ptr p = {asio::detail::addressof(handler),
                          op::ptr::allocate(handler), 0};

    // construct operation
    p.p = new (p.v) op{success_ec_, buffers, remote_addr, handler, io_ex};

    // Optionally register for per-operation cancellation.
    if (slot.is_connected()) {
    }

    // TODO ...
    // ASIO_HANDLER_CREATION((this->context(), *p.p, "rdma", &impl, impl.cm_id_,
    // "async_read"));

    start_read_op(impl, p.p);
    p.v = p.p = 0;
  }

  template <mr_const_buffer_sequence BufferSequence,
            typename Handler, typename IoExecutor>
  void async_write(implementation_type& impl, BufferSequence const& buffers,
                  nd_remote_addr_t const& remote_addr, Handler& handler,
                  IoExecutor const& io_ex) {
    typename asio::associated_cancellation_slot<Handler>::type slot =
        asio::get_associated_cancellation_slot(handler);

    // Allocate and construct an operation to wrap the handler.
    using op = nd_write_op<BufferSequence, Handler, IoExecutor>;
    typename op::ptr p = {asio::detail::addressof(handler),
                          op::ptr::allocate(handler), 0};

    // construct operation
    p.p = new (p.v) op{success_ec_, buffers, remote_addr, handler, io_ex};

    // Optionally register for per-operation cancellation.
    if (slot.is_connected()) {
    }

    // TODO ...
    // ASIO_HANDLER_CREATION((this->context(), *p.p, "rdma", &impl, impl.cm_id_,
    // "async_write"));

    start_write_op(impl, p.p);
    p.v = p.p = 0;
  }

private:
  void close_for_destruction(implementation_type& impl) {
   if (has_state(impl)) {
     // ASIO_HANDLER_OPERATION((context(), "handle", &impl,
     // reinterpret_cast<uintmax_t>(impl.connector_.Get()), "close"));
     impl.state_->qp_.Reset(); 
     impl.state_->cq_.Reset();
     impl.state_->connector_.Reset();
     impl.state_->overlapped_handle_.reset();
     //impl.state_->device_.reset();
   }
  }

  nd_sglist_t& get_sglist() {
    static thread_local nd_sglist_t static_sg_list;
    return static_sg_list;
  }

  void start_connect_op(implementation_type& impl,
                        endpoint_type const& endpoint, nd_connect_op_base* op) {
    this->scheduler_.work_started();
    auto const& state = impl.state_;
    // parse device local endpoint
    using address_type = decltype(endpoint.address());
    endpoint_type endpoint_to_bind{
        address_type::from_string(state->adapter_->name_), endpoint.port()};
    // bind device local endpoint with the connector
    asio::error_code ec{};
    bind_addr(impl, endpoint_to_bind, ec);
    if (ec) {
      this->scheduler_.on_completion(op, ec);
      return;
    }
    auto const& config = state->config_;
    // call overlapped connect interface
    connect(state->connector_.Get(), state->qp_.Get(),
            endpoint.data(), endpoint.size(),
            config.inbound_read_limit_,
            config.outbound_read_limit_, 
            nullptr, 0, op, ec);
    if (ec) {
      this->scheduler_.on_completion(op, ec);
      return;
    }
    // notify this async operation on pending
    this->scheduler_.on_pending(op);
  }

  template <typename RecvOpType>
  void start_recv_op(implementation_type& impl, RecvOpType* op) {
    assert(op);
    auto const& buffers = op->get_buffer_sequence();
    if (all_empty(buffers)) {
      return;
    }

    // TODO... error happens when network direct using local sglist object
    // temp solution: to use the thread local sglist object reference
    nd_sglist_t& sglist = get_sglist();
    // translate buffer sequence to sglist
    buffers2sglist(buffers, sglist);

    // invoke the verbs operation
    verbs_ops::post_recv(impl.state_->qp_.Get(), op, sglist.data(),
                         sglist.size(), op->ec_);
    if (op->ec_) [[unlikely]] {
      post_immediate_completion(op);
    }
    else {
      work_started(impl, op);
    }
  }

  template <typename SendOpType>
  void start_send_op(implementation_type& impl, SendOpType* op) {
    assert(op);
    auto const& buffers = op->get_buffer_sequence();
    if (all_empty(buffers)) {
      return;
    }

    // TODO... error happens when network direct using local sglist object
    // temp solution: to use the thread local sglist object reference
    nd_sglist_t& sglist = get_sglist();
    // translate buffer sequence to sglist
    buffers2sglist(buffers, sglist);

    // invoke the verbs operation
    // TODO ... send flag
    verbs_ops::post_send(impl.state_->qp_.Get(), op, sglist.data(),
                         sglist.size(), 0, op->ec_);
    if (op->ec_) [[unlikely]] {
      post_immediate_completion(op);
    }
    else {
      work_started(impl, op);
    }
  }

  template <typename ReadOpType>
  void start_read_op(implementation_type& impl, ReadOpType* op) {
    assert(op);
    auto const& buffers = op->get_buffer_sequence();
    if (all_empty(buffers)) {
      return;
    }

    // TODO... error happens when network direct using local sglist object
    // temp solution: to use the thread local sglist object reference
    nd_sglist_t& sglist = get_sglist();
    // translate buffer sequence to sglist
    buffers2sglist(buffers, sglist);

    // invoke the verbs operation
    // TODO ... read flag
    auto const& remote_addr = op->get_remote_addr();
    verbs_ops::post_read(impl.state_->qp_.Get(), op, sglist.data(),
                         sglist.size(), remote_addr.addr_, remote_addr.token_,
                         0, op->ec_);
    if (op->ec_) [[unlikely]] {
      post_immediate_completion(op);
    }
    else {
      work_started(impl, op);
    }
  }

  template <typename WriteOpType>
  void start_write_op(implementation_type& impl, WriteOpType* op) {
    assert(op);
    auto const& buffers = op->get_buffer_sequence();
    if (all_empty(buffers)) {
      return;
    }

    // TODO... error happens when network direct using local sglist object
    // temp solution: to use the thread local sglist object reference
    nd_sglist_t& sglist = get_sglist();
    // translate buffer sequence to sglist
    buffers2sglist(buffers, sglist);

    // invoke the verbs operation
    // TODO ... write flag
    auto const& remote_addr = op->get_remote_addr();
    verbs_ops::post_write(impl.state_->qp_.Get(), op, sglist.data(),
                          sglist.size(), remote_addr.addr_, remote_addr.token_,
                          0, op->ec_);
    if (op->ec_) [[unlikely]] {
      post_immediate_completion(op);
    }
    else {
      work_started(impl, op);
    }
  }

  void work_started(implementation_type& impl, nd_notify_wr_op* notify_op) {
    // invoke notify_cq
    asio::error_code ec{};
    native_cq_notify_attr notify_attr{
        .type_ = ND_CQ_NOTIFY_ANY,
        .op_ = notify_op,
    };
    verbs_ops::notify_cq(impl.state_->cq_.Get(), notify_attr, ec);

    // work started
    this->scheduler_.work_started();

    // process on pending
    if (ec && ec == nd_errc::pending) {
      // if notify is on pending, wait for iocp to notify completion
      scheduler_.on_pending(notify_op);
      return;
      // if notify is just in completion, post to scheduler
    }

    // post to scheduler
    this->scheduler_.on_completion(notify_op, ec, 0L);
  }

  void work_started(implementation_type& impl, nd_verbs_op_base* started_op) {
    // using associate allocator to allocate a memory for notify op
    nd_notify_wr_op::Handler handler{};
    nd_notify_wr_op::ptr p = {asio::detail::addressof(handler),
                              nd_notify_wr_op::ptr::allocate(handler),
                              0};
    p.p = new (p.v) nd_notify_wr_op{impl.state_};
    work_started(impl, p.p);
    p.v = p.p = nullptr;
  }

  void post_immediate_completion(nd_verbs_op_base* error_op) {
    // using associate allocator to allocate a memory for error op
    nd_complete_op::Handler handler{};
    nd_complete_op::ptr p = {asio::detail::addressof(handler),
                             nd_complete_op::ptr::allocate(handler),
                             0};
    // placement new on that memory block
    p.p = new (p.v) nd_complete_op{error_op};

    // post to io context, not continuation
    this->scheduler_.post_immediate_completion(p.p, false);
    p.v = p.p = 0;
  }
};

}
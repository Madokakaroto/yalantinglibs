#pragma once

#include "ylt/coro_io/networkdirect/detail/nd_impl_types.hpp"

namespace coro_io {

// command types
enum mr_acccess_flag {
  mr_access_local_write,
  mr_access_remote_read,
  mr_access_remote_write,
};

struct nd2_cq_init_attr {
  HANDLE overlapped_handle_;
  USHORT processor_group_;
  KAFFINITY processor_affinity_;
};

struct nd2_cq_notify_attr {
  ULONG type_;
  LPOVERLAPPED op_;
};

struct nd2_qp_init_attr {
  void* qp_context_;
  IND2CompletionQueue* rcq_;
  IND2CompletionQueue* icq_;
  ULONG max_send_wr_;
  ULONG max_recv_wr_;
  ULONG max_send_sge_;
  ULONG max_recv_sge_;
  ULONG max_inline_data_;
};

// native type definition for the { windows, network-direct } platform
using result_type = HRESULT;
using size_type = ULONG;
using native_context_config_t = ND2_ADAPTER_INFO;
using native_context_t = IND2Adapter;
struct native_pd_t {
  native_context_t* context_;
  detail::unique_handle_t sync_handle_;
};
using native_qp_t = IND2QueuePair;
using native_cq_t = IND2CompletionQueue;
using native_mr_t = IND2MemoryRegion;
using native_sge_t = ND2_SGE;
using native_wc_t = ND2_RESULT;
using native_qp_init_attr = nd2_qp_init_attr;
using native_cq_init_attr = nd2_cq_init_attr;
using native_cq_notify_attr = nd2_cq_notify_attr;

// nd device
struct nd_device_t {
  detail::nd_provider_ptr provider_;
  detail::nd2_adapter_ptr adapter_;
  std::unique_ptr<native_pd_t> pd_;
  std::string name_;
  native_context_config_t info_;
};
using nd_device_ptr = std::shared_ptr<nd_device_t>;
using native_device_t = nd_device_t;
using native_device_ptr = nd_device_ptr;

}  // namespace coro_io
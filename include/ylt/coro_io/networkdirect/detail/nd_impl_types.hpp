#pragma once

namespace coro_io::detail {

// raii handler
struct handle_deleter {
  void operator()(HANDLE handle) const {
    if (handle != INVALID_HANDLE_VALUE || handle != NULL) {
      ::CloseHandle(handle);
    }
  }
};
using unique_handle_t =
    std::unique_ptr<std::remove_pointer_t<HANDLE>, handle_deleter>;

// raii module handler
struct module_deleter {
  void operator()(HMODULE module) const {
    if (module != NULL) {
      ::FreeLibrary(module);
    }
  }
};
using unique_module_t =
    std::unique_ptr<std::remove_pointer_t<HMODULE>, module_deleter>;

// raii buffer
struct scope_buffer {
  void* buffer{nullptr};
  explicit scope_buffer(void* buffer_ptr) : buffer(buffer_ptr) {}
  ~scope_buffer() {
    if (buffer) {
      std::free(buffer);
    }
  }
};

using nd2_adapter_ptr = Microsoft::WRL::ComPtr<IND2Adapter>;
using nd2_provider_ptr = Microsoft::WRL::ComPtr<IND2Provider>;
using nd2_connector_ptr = Microsoft::WRL::ComPtr<IND2Connector>;
using nd2_listener_ptr = Microsoft::WRL::ComPtr<IND2Listener>;
using nd2_queue_pair_ptr = Microsoft::WRL::ComPtr<IND2QueuePair>;
using nd2_completion_queue_ptr = Microsoft::WRL::ComPtr<IND2CompletionQueue>;
using nd2_overlapped_ptr = Microsoft::WRL::ComPtr<IND2Overlapped>;
using nd2_memory_region_ptr = Microsoft::WRL::ComPtr<IND2MemoryRegion>;
using class_factory_ptr = Microsoft::WRL::ComPtr<IClassFactory>;

using dll_can_unload_now = HRESULT (*)(void);
using dll_get_class_object = HRESULT (*)(REFCLSID rclsid, REFIID rrid,
                                         LPVOID* ppv);

struct nd2_sockaddr_t {
  union {
    struct sockaddr src_addr_;
    struct sockaddr_in src_sin_;
    struct sockaddr_in6 src_sin6_;
    struct sockaddr_storage src_storage_;
  };
  size_t address_size_;
  size_t provider_index_;
};

// factory
struct nd_provider_factory_t {
  WSAPROTOCOL_INFOW proto_;
  std::wstring module_name_;
  unique_module_t module_;
  dll_can_unload_now unload_;
  class_factory_ptr factory_;
};
using nd_provider_factory_ptr = std::shared_ptr<nd_provider_factory_t>;

// provider
struct nd_provider_t {
  nd_provider_factory_ptr factory_;
  nd2_provider_ptr provider_;
  size_t index_;
};
using nd_provider_ptr = std::shared_ptr<nd_provider_t>;

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
  IND2CompletionQueue* rcq_;  // receive completion queue
  IND2CompletionQueue* icq_;  // initiator completion queue
  ULONG max_send_wr_;         // max send work requests
  ULONG max_recv_wr_;         // max recv work requests
  ULONG max_send_sge_;        // max send num of scatter/gather elements
  ULONG max_recv_sge_;        // max recv num of scatter/gather elements
  ULONG max_inline_data_;     // max payload data size in a packet
};

// native type definition for the { windows, network-direct } platform

using native_context_t = IND2Adapter;
struct native_pd_t {
  native_context_t* context_;
  unique_handle_t sync_handle_;
};
using native_qp_t = IND2QueuePair;
using native_cq_t = IND2CompletionQueue;
using native_mr_t = IND2MemoryRegion;
using native_sge_t = ND2_SGE;
using native_wc_t = ND2_RESULT;
using native_qp_init_attr = nd2_qp_init_attr;
using native_cq_init_attr = nd2_cq_init_attr;
using native_cq_notify_attr = nd2_cq_notify_attr;

using native_context_config_t = ND2_ADAPTER_INFO;
struct native_device_t {
  nd_provider_ptr provider_;
  nd2_adapter_ptr adapter_;
  std::unique_ptr<native_pd_t> pd_;
  std::string name_;
  native_context_config_t info_;
};
using native_device_ptr = std::shared_ptr<native_device_t>;

// shared state for a rdma connection
struct nd_connector_state_t {
  // is opened
  bool is_opened_;
  // overlapped handle to receive IO completion
  unique_handle_t overlapped_handle_;
  // the network-direect connector interface
  nd2_connector_ptr connector_;
  // the completion queue interface to poll IO work completion
  nd2_completion_queue_ptr cq_;
  // the queue pair interface to perform verbs IO operations
  nd2_queue_pair_ptr qp_;
  // configuration to create this shared state
  nd_connector_config_t config_;
  // device that creates this state
  native_device_ptr device_;
};
using nd_connector_state_ptr = std::shared_ptr<nd_connector_state_t>;

}
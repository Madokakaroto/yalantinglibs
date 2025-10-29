#pragma once

#include <memory>
#include <winnt.h>
#include <wrl/client.h>
#include <libloaderapi.h>
#include <ws2spi.h>
#include <guiddef.h>
#include <ndsupport.h>
#include <ndstatus.h>
#include <ndspi.h>

namespace coro_io {
  using size_type = ULONG;
}

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

// configuration type to initialize the shared state
// TODO ... align with ibverbs
struct nd_connector_config_t {
  size_type cqe_ = 64;
  size_type max_send_wr_ = 32;
  size_type max_recv_wr_ = 32;
  size_type max_send_sge_ = 8;
  size_type max_recv_sge_ = 8;
  size_type max_inline_data_ = 16;
  size_type inbound_read_limit_ = 0;
  size_type outbound_read_limit_ = 0;
};

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
};
using nd_connector_state_ptr = std::shared_ptr<nd_connector_state_t>;

}
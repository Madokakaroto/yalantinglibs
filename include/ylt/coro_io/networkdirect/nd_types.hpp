#pragma once

#include <memory>

#include <winnt.h>
#include <wrl/client.h>
#include <libloaderapi.h>
#include <ws2spi.h>
#include <ndsupport.h>
#include <ndstatus.h>
#include <ndspi.h>

namespace coro_io {

struct handle_deleter {
  void operator()(HANDLE handle) const {
    if (handle != INVALID_HANDLE_VALUE || handle != NULL) {
      ::CloseHandle(handle);
    }
  }
};
using unique_handle_t = std::unique_ptr<std::remove_pointer_t<HANDLE>, handle_deleter>;

struct module_deleter {
  void operator()(HMODULE module) const {
    if (module != NULL) {
      ::FreeLibrary(module);
    }
  }
};
using unique_module_t = std::unique_ptr<std::remove_pointer_t<HMODULE>, module_deleter>;

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
                                         LPVOID *ppv);

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

struct scope_buffer {
  void* buffer{nullptr};
  explicit scope_buffer(void* buffer_ptr)
    : buffer(buffer_ptr) {
  }
  ~scope_buffer() {
    if (buffer) {
      std::free(buffer);
    }
  }
};

}
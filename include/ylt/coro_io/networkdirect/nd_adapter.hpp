#pragma once

#include <ranges>
#include "ylt/coro_io/networkdirect/nd_asio_manual_init.hpp"
#include "ylt/coro_io/networkdirect/nd_types.hpp"
#include "ylt/coro_io/networkdirect/nd_error.hpp"

namespace coro_io {

struct nd_provider_factory_t {
  WSAPROTOCOL_INFOW proto_;
  std::wstring module_name_;
  unique_module_t module_;
  dll_can_unload_now unload_;
  class_factory_ptr factory_;
};
using nd_provider_factory_ptr = std::shared_ptr<nd_provider_factory_t>;

struct nd_provider_t {
  nd_provider_factory_ptr factory_;
  nd2_provider_ptr provider_;
  size_t index_;
};

struct nd_adapter_t {
  nd2_adapter_ptr adapter_;
  std::string name_;
  ND2_ADAPTER_INFO info_;
};

class nd_adapter_manager_t {
 private:
  nd_global_t global_;
  std::vector<nd_provider_t> providers_;
  std::vector<nd_adapter_t> adapters_;

  static bool is_valid_addr(SOCKADDR const& addr);
  static bool is_valid_proto(WSAPROTOCOL_INFOW const& proto);
  static void enumerate_protos(std::vector<WSAPROTOCOL_INFOW>& out_protos,
                               std::error_code& ec);
  static std::vector<WSAPROTOCOL_INFOW> enumerate_protos();
  static std::wstring get_provider_path(WSAPROTOCOL_INFOW const& proto,
                                        std::error_code& ec);
  static auto create_provider_factory(std::wstring provider_path,
                               WSAPROTOCOL_INFOW const& proto,
                               std::error_code& ec) -> nd_provider_factory_ptr;
  static auto create_provider(nd_provider_factory_t const& factory,
                              std::error_code& ec) -> nd2_provider_ptr;
  static void enumerate_addr_list(nd_provider_t const& provider,
                                  std::vector<nd2_sockaddr_t>& addr_list,
                                  std::error_code& ec);
  static auto enumerate_addr_list(nd_provider_t const& provider)
      -> std::vector<nd2_sockaddr_t>;
  static auto open_adapter(nd2_provider_ptr const& ptr, sockaddr const* addrin,
                    std::size_t addr_size, asio::error_code& ec)
      -> nd2_adapter_ptr;
  static ND2_ADAPTER_INFO query_adapter_info(nd2_adapter_ptr const& adaptor,
                                             asio::error_code& ec);
  static std::string query_adapter_name(ND2_ADAPTER_INFO const& info,
                                           sockaddr* addrin,
                                           std::size_t addr_size,
                                           asio::error_code& ec);
  static std::vector<nd_provider_t> get_providers();
  static std::vector<nd_adapter_t> create_adapters(std::vector<nd_provider_t> const& providers);

  nd_adapter_manager_t()
      : global_()
      , providers_(get_providers())
      , adapters_() {
  }

 public:
  static nd_adapter_manager_t const& instance() {
    static nd_adapter_manager_t instance{};
    return instance;
  }
};

inline bool nd_adapter_manager_t::is_valid_addr(SOCKADDR const& addr) {
  switch (addr.sa_family) {
    case AF_INET: {
      sockaddr_in const& addr4 = reinterpret_cast<sockaddr_in const&>(addr);
      /* HACK-alert: reject local or MS default IPv4 addrs */
      return !(addr4.sin_addr.S_un.S_un_b.s_b1 == 169 ||
               addr4.sin_addr.S_un.S_un_b.s_b1 == 127);
    }
    case AF_INET6: {
      sockaddr_in6 const& addr6 = reinterpret_cast<sockaddr_in6 const&>(addr);
      return !(addr6.sin6_addr.u.Byte[0] == 0xfe &&
               addr6.sin6_addr.u.Byte[1] == 0x80);
    }
    default:
      break;
  }
  return false;
}

inline bool nd_adapter_manager_t::is_valid_proto(WSAPROTOCOL_INFOW const& proto) {
  constexpr auto fi_nd_proto_flag = XP1_GUARANTEED_DELIVERY |
                                    XP1_GUARANTEED_ORDER |
                                    XP1_MESSAGE_ORIENTED | XP1_CONNECT_DATA;
  if ((proto.dwServiceFlags1 & fi_nd_proto_flag) != fi_nd_proto_flag) {
    return false;
  }
  if (!(proto.iAddressFamily == AF_INET || proto.iAddressFamily == AF_INET6)) {
    return false;
  }
  if (proto.iSocketType != -1) {
    return false;
  }
  if (proto.iProtocol || proto.iProtocolMaxOffset) {
    return false;
  }
  return proto.iVersion == NDVER;
}

inline void nd_adapter_manager_t::enumerate_protos(
    std::vector<WSAPROTOCOL_INFOW>& out_protos,
    std::error_code& ec) {
  DWORD proto_len = 0;
  int err = 0;

  HRESULT hr = ::WSCEnumProtocols(nullptr, nullptr, &proto_len, &err);
  if (hr != SOCKET_ERROR || err != WSAENOBUFS) {
    ec = std::error_code{ err, std::system_category() };
    return;
  }

  std::size_t const array_size = proto_len / sizeof(WSAPROTOCOL_INFOW);
  std::vector<WSAPROTOCOL_INFOW> result{};
  result.resize(array_size);
  hr = ::WSCEnumProtocols(nullptr, result.data(), &proto_len, &err);
  if (FAILED(hr)) {
    ec = std::error_code{err, std::system_category()};
    return;
  }

  auto itr = std::remove_if(result.begin(), result.end(),
                            [](WSAPROTOCOL_INFOW const& elem) {
                              return !is_valid_proto(elem);
                            });
  result.erase(itr, result.end());

  out_protos = std::move(result);
  ec.clear();
}

inline std::vector<WSAPROTOCOL_INFOW> nd_adapter_manager_t::enumerate_protos() {
  std::vector<WSAPROTOCOL_INFOW> result{};
  std::error_code ec{};
  enumerate_protos(result, ec);
  throw_error(ec);
  return result;
}

inline std::wstring nd_adapter_manager_t::get_provider_path(
    WSAPROTOCOL_INFOW const& proto,
    std::error_code& ec) {
  int len = 0, err = 0, res = 0;

  res = WSCGetProviderPath((GUID*)&proto.ProviderId, NULL, &len, &err);
  if (res != SOCKET_ERROR || err != WSAEFAULT) {
    ec = make_system_error_code(err);
    return {};
  }
  std::wstring temp{};
  temp.resize(len);
  res = WSCGetProviderPath((GUID*)&proto.ProviderId, temp.data(), &len, &err);
  if (res != 0) {
    if (res == WSAEINVAL) {
      ec = make_error_code(std::errc::invalid_argument);
    }
    else  // WSAEFAULT
    {
      ec = make_error_code(std::errc::not_enough_memory);
    }
    return {};
  }

  len = ExpandEnvironmentStringsW(temp.c_str(), NULL, 0);
  if (len == 0) {
    ec = make_system_error_code(::GetLastError());
    return {};
  }

  std::wstring result{};
  result.resize(len);
  len = ExpandEnvironmentStringsW(temp.c_str(), result.data(), len);
  if (len == 0) {
    ec = make_system_error_code(::GetLastError());
    return {};
  }

  return result;
}

inline auto nd_adapter_manager_t::create_provider_factory(
    std::wstring provider_path,
    WSAPROTOCOL_INFOW const& proto, asio::error_code& ec)
    -> nd_provider_factory_ptr {
  unique_module_t provier_module{ LoadLibraryW(provider_path.c_str()) };
  if (!provier_module) {
    ec = make_system_error_code(::GetLastError());
    return nullptr;
  }

  dll_can_unload_now unload = reinterpret_cast<dll_can_unload_now>(
      GetProcAddress(provier_module.get(), "DllCanUnloadNow"));
  if (!unload) {
    ec = make_system_error_code(::GetLastError());
    return nullptr;
  }

  dll_get_class_object getclassobj = reinterpret_cast<dll_get_class_object>(
      GetProcAddress(provier_module.get(), "DllGetClassObject"));
  if (!getclassobj) {
    ec = make_system_error_code(::GetLastError());
    return nullptr;
  }

  class_factory_ptr class_factory{};
  HRESULT hr =
      getclassobj(proto.ProviderId, IID_IClassFactory,
                  reinterpret_cast<LPVOID*>(class_factory.GetAddressOf()));
  if (hr != S_OK) {
    ec = make_system_error_code(hr);
    return nullptr;
  }

  auto provider_factory = std::make_shared<nd_provider_factory_t>();
  provider_factory->proto_ = proto;
  provider_factory->module_name_ = std::move(provider_path);
  provider_factory->module_ = std::move(provier_module);
  provider_factory->unload_ = unload;
  provider_factory->factory_ = std::move(class_factory);
  return provider_factory;
}

auto nd_adapter_manager_t::create_provider(nd_provider_factory_t const& factory,
                                         std::error_code& ec) -> nd2_provider_ptr {
  nd2_provider_ptr provdier{};
  HRESULT const hr = factory.factory_->CreateInstance(
      nullptr, IID_IND2Provider,
      reinterpret_cast<LPVOID*>(provdier.GetAddressOf()));
  if (hr != S_OK) {
    ec = make_system_error_code(hr);
    return nullptr;
  }
  return provdier;
}

inline void nd_adapter_manager_t::enumerate_addr_list(
    nd_provider_t const& provider,
    std::vector<nd2_sockaddr_t>& addr_list,
    std::error_code& ec) {
  ULONG addr_list_buffer_size{ 0ul };
  provider.provider_->QueryAddressList(nullptr, &addr_list_buffer_size);
  if (addr_list_buffer_size == 0) {
    ec = make_error_code(std::errc::address_not_available);
    return;
  }

  scope_buffer buffer{std::malloc(addr_list_buffer_size)};
  if (!buffer.buffer) {
    ec = make_error_code(std::errc::not_enough_memory);
    return;
  }
  SOCKET_ADDRESS_LIST* temp_addr_list =
      static_cast<SOCKET_ADDRESS_LIST*>(buffer.buffer);
  HRESULT hr = provider.provider_->QueryAddressList(temp_addr_list,
                                                    &addr_list_buffer_size);
  if (hr != ND_SUCCESS) {
    ec = make_nd_error_code(hr);
    return;
  }
  if (temp_addr_list->iAddressCount <= 0) {
    ec = make_error_code(std::errc::address_not_available);
    return;
  }

  auto addr_range = std::ranges::subrange{
      temp_addr_list->Address,
      temp_addr_list->Address + temp_addr_list->iAddressCount}
  | std::views::filter([](auto const& sock_addr) {
    // TODO... config
        return sock_addr.lpSockaddr->sa_family == AF_INET;
  }) | std::views::transform([&](auto const& sock_addr) {
        nd2_sockaddr_t result{};
        std::memcpy(&result.src_addr_, sock_addr.lpSockaddr,
                    sock_addr.iSockaddrLength);
        result.provider_index_ = provider.index_;
    return result;
  });
  std::vector<nd2_sockaddr_t> result{
    addr_range.begin(), addr_range.end()};

  addr_list = std::move(result);
  ec.clear();
}

inline auto nd_adapter_manager_t::enumerate_addr_list(
    nd_provider_t const& provider)
    -> std::vector<nd2_sockaddr_t> {
  std::vector<nd2_sockaddr_t> result{};
  std::error_code ec{};
  enumerate_addr_list(provider, result, ec);
  throw_error(ec);
  return result;
}

inline auto nd_adapter_manager_t::open_adapter(
    nd2_provider_ptr const& provider, sockaddr const* addrin,
    std::size_t addr_size, asio::error_code& ec)
    -> nd2_adapter_ptr {
  UINT64 adaptor_id = 0;
  HRESULT hr = provider->ResolveAddress(addrin, static_cast<ULONG>(addr_size),
                                        &adaptor_id);
  if (hr != ND_SUCCESS) {
    ec = make_nd_error_code(hr);
    return nullptr;
  }

  nd2_adapter_ptr adapter{};
  hr = provider->OpenAdapter(IID_IND2Adapter, adaptor_id,
                             reinterpret_cast<LPVOID*>(adapter.GetAddressOf()));
  if (hr != ND_SUCCESS) {
    ec = make_nd_error_code(hr);
    return nullptr;
  }

  return adapter;
}

inline ND2_ADAPTER_INFO nd_adapter_manager_t::query_adapter_info(
    nd2_adapter_ptr const& adaptor,
    asio::error_code& ec) {
  assert(adaptor);

  ND2_ADAPTER_INFO result = {0};
  result.InfoVersion = ND_VERSION_2;
  ULONG linfo = sizeof(result);
  HRESULT hr = adaptor->Query(&result, &linfo);
  if (hr != ND_SUCCESS) {
    ec = make_nd_error_code(hr);
  }
  return result;
}

inline std::string nd_adapter_manager_t::query_adapter_name(
    ND2_ADAPTER_INFO const& info,
    sockaddr* addrin, std::size_t addr_size,
    asio::error_code& ec) {
  std::string result{};
  DWORD addrlen = 0;
#if defined(_MSC_VER) && (_MSC_VER >= 1800)
  int res = WSAAddressToStringW(addrin, static_cast<DWORD>(addr_size), NULL,
                                NULL, &addrlen);
  if (res != 0) {
    ec = make_system_error_code(::WSAGetLastError());
  }
  if (res == SOCKET_ERROR && ec.value() == WSAEFAULT && addrlen != 0) {
    LPWSTR string_buffer = (LPWSTR)_alloca(addrlen * sizeof(WCHAR));
    res = WSAAddressToStringW(addrin, static_cast<DWORD>(addr_size), NULL,
                              string_buffer, &addrlen);
    if (res != 0) {
      ec = make_system_error_code(::WSAGetLastError());
    }
    if (!ec) {
      res = ::WideCharToMultiByte(CP_ACP, 0, string_buffer, -1, NULL, 0, 0, 0);
      if (res == 0) {
        ec = asio::error_code{static_cast<int>(::GetLastError()),
                              asio::error::get_system_category()};
      }
      else {
        result.resize(res);
        res = ::WideCharToMultiByte(CP_ACP, 0, string_buffer, -1, result.data(),
                                    res, 0, 0);
        if (res == 0) {
          ec = asio::error_code{static_cast<int>(::GetLastError()),
                                asio::error::get_system_category()};
        }
      }
    }
  }
#else
  int res = WSAAddressToStringA(addrin, static_cast<DWORD>(addr_size), NULL,
                                NULL, &addrlen);
  if (res != 0) {
    ec = make_system_error_code(::WSAGetLastError());
  }

  if (res == SOCKET_ERROR && ec.value() == WSAEFAULT && addrlen != 0) {
    result.resize(addrlen);
    res = WSAAddressToStringA(addrin, static_cast<DWORD>(addr_size), NULL,
                              result.data(), &addrlen);
    if (res != 0) {
      ec = make_system_error_code(::WSAGetLastError());
    }
  }
#endif
  return result;
}

inline std::vector<nd_provider_t> nd_adapter_manager_t::get_providers() {
  auto providers =
    enumerate_protos() |
      std::views::transform([index{size_t{0}}](auto const& proto) mutable {
        std::error_code ec{};
        nd_provider_t result{};
        auto const provider_path = get_provider_path(proto, ec);
        if (!ec) {
          return result;
        }

        auto provider_factory = create_provider_factory(
          provider_path, proto, ec);
        if (!ec) {
          return result;
        }

        auto provider = create_provider(*provider_factory, ec);
        if (!ec) {
          return result;
        }

        return result = {
          .factory_ = provider_factory,
          .provider_ = provider,
          .index_ = index++,
        };
      }) | std::views::filter([](auto const& provider) {
        return provider.provider_ != nullptr;
      });

  return {std::ranges::begin(providers), std::ranges::end(providers) };
}

inline std::vector<nd_adapter_t> nd_adapter_manager_t::create_adapters(
  std::vector<nd_provider_t> const& providers) {
  auto adapters = providers | std::views::transform([](auto const& provider) {
    return enumerate_addr_list(provider);
  }) | std::views::join | std::views::transform([&](auto& addr) {
    std::error_code ec{};
    nd_adapter_t result{};
    auto adapter_ptr =
        open_adapter(providers[addr.provider_index_].provider_, &addr.src_addr_,
                     addr.address_size_, ec);
    if (ec) {
      return result;
    }

    auto const adapter_info = query_adapter_info(adapter_ptr, ec);
    if (ec) {
      return result;
    }

    auto const adapter_name = query_adapter_name(
        adapter_info, &addr.src_addr_, addr.address_size_, ec);
    if (ec) {
      return result;
    }

    return result = {
      .adapter_ = adapter_ptr,
      .name_ = adapter_name,
      .info_ = adapter_info,
    };
  }) | std::views::filter([](nd_adapter_t const& adapter) {
    return adapter.adapter_ != nullptr;
  });

  std::vector<nd_adapter_t> result{};
  std::ranges::for_each(adapters, [&result](nd_adapter_t const& adapter) {
    result.push_back(adapter);
  });
  return result;
}

}
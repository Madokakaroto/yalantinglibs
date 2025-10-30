#pragma once

#include "ylt/easylog.hpp"
#include "ylt/coro_io/networkdirect/nd_device.hpp"
#include "ylt/coro_io/networkdirect/detail/nd_ops_verbs.hpp"

namespace coro_io {

class nd_mr {
 private:
  detail::nd2_memory_region_ptr mr_;
  void* addr_;
  std::size_t length_;
  mr_acccess_flag_t flag_;
  int extra_flag_;

 public:
  explicit nd_mr(nd_device_ptr const& device, void* addr, std::size_t length,
                 mr_acccess_flag_t flag = mr_access_remote_write,
                 int extra_flag = 0)
      : mr_(throw_reg_mr(device, addr, length, flag, extra_flag)),
        addr_(addr),
        length_(length),
        flag_(flag),
        extra_flag_(extra_flag) {}

  ~nd_mr() {
    asio::error_code ec{};
    detail::verbs_ops::dereg_mr(mr_.Get(), ec);
    if (ec) {
      ELOG_CRITICAL << "Failed to deregister memory region, errc(" << ec.value()
                    << ") message(" << ec.message();
    }
  }

  nd_mr(nd_mr const&) = delete;
  nd_mr& operator=(nd_mr const&) = delete;
  nd_mr(nd_mr&&) = default;
  nd_mr& operator=(nd_mr&&) = default;

  std::uint32_t local_key() const {
    if (!mr_) {
      asio::detail::throw_error(nd_errc::ndext_invalid_mr);
    }
    return mr_->GetLocalToken();
  }

  std::uint32_t remote_key() const {
    if (!mr_) {
      asio::detail::throw_error(nd_errc::ndext_invalid_mr);
    }
    return mr_->GetRemoteToken();
  }

  bool is_in_mr(void const* addr, std::size_t length) const noexcept {
    if (!mr_) {
      return false;
    }
    auto const diff = std::distance(static_cast<char const*>(addr_),
                                    static_cast<char const*>(addr));
    if (diff < 0 || diff >= length_) {
      return false;
    }
    auto const length_from_addr = diff + length;
    if (length_from_addr >= length_) {
      return false;
    }
  }

 private:
  detail::nd2_memory_region_ptr throw_reg_mr(nd_device_ptr const& device,
                                             void* addr, std::size_t length,
                                             mr_acccess_flag_t flag,
                                             int extra_flag) {
    asio::error_code ec{};
    detail::nd2_memory_region_ptr result{detail::verbs_ops::reg_mr(
        device->pd_.get(), addr, length, flag, extra_flag, ec)};
    asio::detail::throw_error(ec);
    return result;
  }

 public:
  class const_buffer {
   private:
    nd_mr const& mr_;
    void const* addr_;
    std::size_t length_;

   public:
    explicit const_buffer(nd_mr const& mr)
        : mr_(mr), addr_(nullptr), length_(0) {}
    const_buffer(nd_mr const& mr, void const* addr, size_t length)
        : mr_(mr), addr_(addr), length_(length) {}
    ~const_buffer() = default;
    const_buffer(const_buffer const&) = default;
    const_buffer& operator=(const_buffer const&) = default;
    const_buffer(const_buffer&&) = default;
    const_buffer& operator=(const_buffer&&) = default;

   public:
    void const* addr() const noexcept { return addr_; }

    size_t length() const noexcept { return length_; }

    bool is_valid() const noexcept {
      return addr_ != nullptr && length_ >= 0 && mr_.is_in_mr(addr_, length_);
    }

    nd_mr const& get_mr() const noexcept { return mr_; }

    std::uint32_t local_key() const { return get_mr().local_key(); }

    std::uint32_t remote_key() const { return get_mr().remote_key(); }

    friend const_buffer const* buffer_sequence_begin(
        const_buffer const& one_buffer) noexcept {
      return std::addressof(one_buffer);
    }

    friend const_buffer const* buffer_sequence_end(
        const_buffer const& one_buffer) noexcept {
      return std::addressof(one_buffer) + 1;
    }
  };

  class mutable_buffer {
   private:
    nd_mr const& mr_;
    void* addr_;
    std::size_t length_;

   public:
    explicit mutable_buffer(nd_mr const& mr)
        : mr_(mr), addr_(nullptr), length_(0) {}
    mutable_buffer(nd_mr const& mr, void* addr, size_t length)
        : mr_(mr), addr_(addr), length_(length) {}
    ~mutable_buffer() = default;
    mutable_buffer(mutable_buffer const&) = default;
    mutable_buffer& operator=(mutable_buffer const&) = default;
    mutable_buffer(mutable_buffer&&) = default;
    mutable_buffer& operator=(mutable_buffer&&) = default;

   public:
    void* addr() const noexcept { 
      return addr_;
    }

    size_t length() const noexcept {
      return length_;
    }

    bool is_valid() const noexcept {
      return addr_ != nullptr && length_ >= 0 && mr_.is_in_mr(addr_, length_);
    }

    nd_mr const& get_mr() const noexcept { 
      return mr_;
    }

    std::uint32_t local_key() const {
      return get_mr().local_key();
    }

    std::uint32_t remote_key() const {
      return get_mr().remote_key();
    }

    friend mutable_buffer const* buffer_sequence_begin(
        mutable_buffer const& one_buffer) noexcept {
      return std::addressof(one_buffer);
    }

    friend mutable_buffer const* buffer_sequence_end(
        mutable_buffer const& one_buffer) noexcept {
      return std::addressof(one_buffer) + 1;
    }
  };
};

}
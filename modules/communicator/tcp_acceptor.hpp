#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include <paracooba/common/status.h>

namespace boost {
namespace system {
class error_code;
}
namespace asio::ip {
class address;
}
}

struct parac_handle;

namespace parac::communicator {
class Service;

class TCPAcceptor {
  public:
  TCPAcceptor();
  ~TCPAcceptor();

  parac_status start(Service& service,
                     const std::string& listenAddressStr,
                     uint16_t listenPort);

  std::string listenAddress() const;
  std::string connectionString() const;

  boost::asio::ip::address address() const;

  private:
  struct Internal;
  std::unique_ptr<Internal> m_internal;

  void loop(const boost::system::error_code& ec);
};
}

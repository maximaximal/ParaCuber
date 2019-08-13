#ifndef PARACUBER_NETWORKED_NODE_HPP
#define PARACUBER_NETWORKED_NODE_HPP

#include <boost/asio/ip/udp.hpp>

namespace paracuber {
class NetworkedNode
{
  public:
  explicit NetworkedNode(boost::asio::ip::udp::endpoint remoteEndoint)
    : m_remoteEndoint(remoteEndoint)
  {}
  ~NetworkedNode() {}

  boost::asio::ip::udp::endpoint getRemoteEndpoint() { return m_remoteEndoint; }

  void setPort(uint16_t p) { m_remoteEndoint.port(p); }

  private:
  boost::asio::ip::udp::endpoint m_remoteEndoint;
};
}

#endif

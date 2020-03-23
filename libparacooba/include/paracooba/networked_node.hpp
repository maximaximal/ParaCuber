#ifndef PARACOOBA_NETWORKED_NODE_HPP
#define PARACOOBA_NETWORKED_NODE_HPP

#include <atomic>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ip/udp.hpp>

#include "messages/jobdescription_transmitter.hpp"
#include "messages/message_receiver.hpp"
#include "types.hpp"

namespace paracooba {
namespace net {
class Connection;
}

class NetworkedNode
  : public messages::JobDescriptionTransmitter
  , public messages::MessageTransmitter
{
  public:
  explicit NetworkedNode(
    ID id,
    messages::MessageTransmitter& statelessMessageTransmitter);
  virtual ~NetworkedNode();

  const boost::asio::ip::udp::endpoint& getRemoteUdpEndpoint() const
  {
    return m_remoteUdpEndoint;
  }
  const boost::asio::ip::tcp::endpoint& getRemoteTcpEndpoint() const
  {
    return m_remoteTcpEndoint;
  }

  void setRemoteUdpEndpoint(boost::asio::ip::udp::endpoint endpoint)
  {
    m_remoteUdpEndoint = endpoint;
    m_udpEndpointSet = true;
  }
  void setRemoteTcpEndpoint(boost::asio::ip::tcp::endpoint endpoint)
  {
    m_remoteTcpEndoint = endpoint;
    m_tcpEndpointSet = true;
  }

  virtual void transmitMessage(const messages::Message& jd,
                               NetworkedNode& nn,
                               SuccessCB sendFinishedCB);

  virtual void transmitJobDescription(messages::JobDescription&& jd,
                                      int64_t id,
                                      SuccessCB sendFinishedCB);

  void setUdpPort(uint16_t p)
  {
    m_remoteUdpEndoint.port(p);
    m_udpPortSet = true;
  }
  void setTcpPort(uint16_t p)
  {
    m_remoteTcpEndoint.port(p);
    m_tcpPortSet = true;
  }
  int64_t getId() const { return m_id; }

  inline std::ostream& operator<<(std::ostream& o) { return o << getId(); }

  void addActiveTCPClient() { ++m_activeTCPClients; }
  void removeActiveTCPClient() { --m_activeTCPClients; }
  bool hasActiveTCPClients() { return m_activeTCPClients > 0; }
  bool deletionRequested() { return m_deletionRequested; }
  void requestDeletion() { m_deletionRequested = true; }

  bool assignConnection(const net::Connection& conn);

  bool isUdpEndpointSet() const { return m_udpEndpointSet; }
  bool isTcpEndpointSet() const { return m_tcpEndpointSet; }
  bool isUdpPortSet() const { return m_udpPortSet; }
  bool isTcpPortSet() const { return m_tcpPortSet; }

  private:
  boost::asio::ip::udp::endpoint m_remoteUdpEndoint;
  boost::asio::ip::tcp::endpoint m_remoteTcpEndoint;
  int64_t m_id;

  bool m_udpEndpointSet = false;
  bool m_tcpEndpointSet = false;
  bool m_udpPortSet = false;
  bool m_tcpPortSet = false;

  std::atomic_size_t m_activeTCPClients = 0;
  std::atomic_bool m_deletionRequested = false;

  messages::MessageTransmitter& m_statelessMessageTransmitter;
  std::unique_ptr<net::Connection> m_connection;
};
}

#endif

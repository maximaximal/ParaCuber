#ifndef PARACOOBA_NETWORKED_NODE_HPP
#define PARACOOBA_NETWORKED_NODE_HPP

#include <atomic>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ip/udp.hpp>
#include <ostream>
#include <pthread.h>

#include "messages/jobdescription_transmitter.hpp"
#include "messages/message_receiver.hpp"
#include "readywaiter.hpp"
#include "types.hpp"

namespace paracooba {
namespace net {
class Connection;
}

class ClusterNode;

class NetworkedNode
  : public messages::JobDescriptionTransmitter
  , public messages::MessageTransmitter
{
  public:
  using ConnectionReadyWaiter = ReadyWaiter<net::Connection>;

  explicit NetworkedNode(
    ID id,
    messages::MessageTransmitter& statelessMessageTransmitter,
    ClusterNode& clusterNode);
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
    updateRemoteConnectionString();
  }

  const std::string& getRemoteConnectionString() const
  {
    static const std::string defaultRemoteConnectionString = "";
    if(!m_tcpEndpointSet || !m_tcpPortSet)
      return defaultRemoteConnectionString;
    return m_remoteConnectionString;
  }
  void updateRemoteConnectionString();

  virtual void transmitMessage(const messages::Message& msg,
                               NetworkedNode& nn,
                               SuccessCB sendFinishedCB = EmptySuccessCB);

  virtual void transmitJobDescription(messages::JobDescription&& jd,
                                      NetworkedNode& nn,
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
    updateRemoteConnectionString();
  }
  int64_t getId() const { return m_id; }

  void addActiveTCPClient() { ++m_activeTCPClients; }
  void removeActiveTCPClient()
  {
    if(m_activeTCPClients > 0)
      --m_activeTCPClients;
  }
  bool hasActiveTCPClients() { return m_activeTCPClients > 0; }
  bool deletionRequested() { return m_deletionRequested; }
  void requestDeletion();

  bool isConnectionReady() const;
  bool assignConnection(const net::Connection& conn);
  void resetConnection();
  net::Connection* getConnection() { return m_connection.get(); }
  ConnectionReadyWaiter& getConnectionReadyWaiter()
  {
    return m_connectionReadyWaiter;
  }

  bool isUdpEndpointSet() const { return m_udpEndpointSet; }
  bool isTcpEndpointSet() const { return m_tcpEndpointSet; }
  bool isUdpPortSet() const { return m_udpPortSet; }
  bool isTcpPortSet() const { return m_tcpPortSet; }

  ClusterNode& getClusterNode() const { return *m_clusterNode; }

  private:
  friend class ClusterNode;
  void setClusterNode(ClusterNode& clusterNode)
  {
    m_clusterNode = &clusterNode;
  }

  boost::asio::ip::udp::endpoint m_remoteUdpEndoint;
  boost::asio::ip::tcp::endpoint m_remoteTcpEndoint;
  int64_t m_id;

  bool m_udpEndpointSet = false;
  bool m_tcpEndpointSet = false;
  bool m_udpPortSet = false;
  bool m_tcpPortSet = false;

  std::atomic_size_t m_activeTCPClients = 0;
  std::atomic_bool m_deletionRequested = false;

  std::string m_remoteConnectionString;

  messages::MessageTransmitter& m_statelessMessageTransmitter;
  std::unique_ptr<net::Connection> m_connection;
  ConnectionReadyWaiter m_connectionReadyWaiter;

  ClusterNode* m_clusterNode;
};

std::ostream&
operator<<(std::ostream& o, const NetworkedNode& n);
}

#endif

#ifndef PARACOOBA_NET_UDPSERVER
#define PARACOOBA_NET_UDPSERVER

#include <memory>
#include <mutex>

#include <boost/asio/coroutine.hpp>
#include <boost/asio/io_service.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ip/udp.hpp>
#include <boost/asio/streambuf.hpp>

#include "../log.hpp"
#include "../messages/message_receiver.hpp"

namespace paracooba {
class ClusterNode;
class ClusterNodeStore;

namespace net {
class UDPServer : public messages::MessageTransmitter
{
  public:
  struct State
  {
    State(boost::asio::io_service& ioService,
          boost::asio::ip::udp::endpoint endpoint,
          boost::asio::ip::udp::endpoint broadcastEndpoint,
          ConfigPtr config,
          LogPtr log,
          messages::MessageReceiver& messageReceiver);
    ~State();

    boost::asio::ip::udp::socket socket;
    boost::asio::ip::udp::endpoint broadcastEndpoint;
    boost::asio::ip::udp::endpoint remoteEndpoint;
    boost::asio::streambuf recvStreambuf;
    boost::asio::streambuf sendStreambuf;
    ConfigPtr config;
    Logger logger;
    messages::MessageReceiver& messageReceiver;
    ClusterNodeStore* clusterNodeStore = nullptr;
    ClusterNode* thisNode = nullptr;
    std::mutex sendMutex;
    boost::asio::coroutine readCoro;
  };

  UDPServer(boost::asio::io_service& ioService,
            boost::asio::ip::udp::endpoint endpoint,
            boost::asio::ip::udp::endpoint broadcastEndpoint,
            ConfigPtr config,
            LogPtr log,
            messages::MessageReceiver& messageReceiver);
  virtual ~UDPServer();

  void startAccepting(ClusterNodeStore& clusterNodeStore,
                      ClusterNode& thisNode);

  virtual void transmitMessage(const messages::Message& msg,
                               NetworkedNode& nn,
                               SuccessCB sendFinishedCB = EmptySuccessCB);

  void broadcastMessage(const messages::Message& msg,
                        SuccessCB successCB = EmptySuccessCB);

  void transmitMessageToEndpoint(const messages::Message& msg,
                                 boost::asio::ip::udp::endpoint target,
                                 SuccessCB sendFinishedCB = EmptySuccessCB);

  /** @brief Read handler that is called when data is received. */
  void operator()(const boost::system::error_code& ec, size_t bytes_received);

  private:
  std::shared_ptr<State> m_state;

  void accept();
  void enrichLogger();

  boost::asio::ip::udp::socket& socket() { return m_state->socket; }
  boost::asio::ip::udp::endpoint& remoteEndpoint()
  {
    return m_state->remoteEndpoint;
  }
  boost::asio::ip::udp::endpoint& broadcastEndpoint()
  {
    return m_state->broadcastEndpoint;
  }
  boost::asio::streambuf& recvStreambuf() { return m_state->recvStreambuf; }
  boost::asio::streambuf& sendStreambuf() { return m_state->sendStreambuf; }
  boost::asio::coroutine& readCoro() { return m_state->readCoro; }
  Logger& logger() { return m_state->logger; }
  messages::MessageReceiver& messageReceiver()
  {
    return m_state->messageReceiver;
  }
  ClusterNodeStore& clusterNodeStore()
  {
    assert(m_state->clusterNodeStore);
    return *m_state->clusterNodeStore;
  }
  std::mutex& sendMutex() { return m_state->sendMutex; }
};
}
}

#endif

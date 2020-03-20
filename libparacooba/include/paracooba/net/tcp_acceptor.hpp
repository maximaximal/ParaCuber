#ifndef PARACOOBA_NET_TCP_ACCEPTOR
#define PARACOOBA_NET_TCP_ACCEPTOR

#include <boost/system/error_code.hpp>
#include <memory>

#include <boost/asio/coroutine.hpp>
#include <boost/asio/io_service.hpp>
#include <boost/asio/ip/tcp.hpp>

#include "../log.hpp"

namespace paracooba {
namespace messages {
class MessageReceiver;
class JobDescriptionReceiverProvider;
}

namespace net {
class Connection;

/** @brief Accepts incoming TCP Connections as paracooba::net::Connection
 * objects.
 *
 * Initializes these connections with the respective required handlers and
 * assigns them to the global paracooba::NetworkedNode instances. */
class TCPAcceptor : boost::asio::coroutine
{
  public:
  struct State
  {
    explicit State(
      boost::asio::io_service& ioService,
      boost::asio::ip::tcp::endpoint endpoint,
      LogPtr log,
      ConfigPtr config,
      messages::MessageReceiver& msgReceiver,
      messages::JobDescriptionReceiverProvider& jdReceiverProvider);

    boost::asio::io_service& ioService;
    boost::asio::ip::tcp::acceptor acceptor;
    LogPtr log;
    Logger logger;
    ConfigPtr config;
    messages::MessageReceiver& msgReceiver;
    messages::JobDescriptionReceiverProvider& jdReceiverProvider;

    std::unique_ptr<Connection> newConnection;
  };

  TCPAcceptor(boost::asio::io_service& ioService,
              boost::asio::ip::tcp::endpoint endpoint,
              LogPtr log,
              ConfigPtr config,
              messages::MessageReceiver& msgReceiver,
              messages::JobDescriptionReceiverProvider& jdReceiverProvider);
  ~TCPAcceptor();

  void startAccepting();

  /** @brief Accept handler that is called when new connections arrive. */
  void operator()(const boost::system::error_code& ec);

  private:
  std::shared_ptr<State> m_state;

  boost::asio::io_service& ioService() { return m_state->ioService; }
  boost::asio::ip::tcp::acceptor& acceptor() { return m_state->acceptor; }
  LogPtr& log() { return m_state->log; }
  Logger& logger() { return m_state->logger; }
  ConfigPtr& config() { return m_state->config; }
  messages::MessageReceiver& messageReceiver() { return m_state->msgReceiver; }
  messages::JobDescriptionReceiverProvider& jobDescriptionReceiverProvider()
  {
    return m_state->jdReceiverProvider;
  }
  std::unique_ptr<Connection>& newConnection()
  {
    return m_state->newConnection;
  };

  void makeNewConnection();
};
}
}

#endif

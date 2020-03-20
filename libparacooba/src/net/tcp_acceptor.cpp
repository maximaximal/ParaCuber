#include "../../include/paracooba/net/tcp_acceptor.hpp"
#include "../../include/paracooba/net/connection.hpp"
#include <boost/system/error_code.hpp>

namespace paracooba {
namespace net {
TCPAcceptor::State::State(
  boost::asio::io_service& ioService,
  boost::asio::ip::tcp::endpoint endpoint,
  LogPtr log,
  ConfigPtr config,
  ClusterNodeStore& clusterNodeStore,
  messages::MessageReceiver& msgReceiver,
  messages::JobDescriptionReceiverProvider& jdReceiverProvider)
  : ioService(ioService)
  , acceptor(ioService, endpoint)
  , log(log)
  , logger(log->createLogger("TCPAcceptor"))
  , config(config)
  , clusterNodeStore(clusterNodeStore)
  , msgReceiver(msgReceiver)
  , jdReceiverProvider(jdReceiverProvider)
{}

TCPAcceptor::TCPAcceptor(
  boost::asio::io_service& ioService,
  boost::asio::ip::tcp::endpoint endpoint,
  LogPtr log,
  ConfigPtr config,
  ClusterNodeStore& clusterNodeStore,
  messages::MessageReceiver& msgReceiver,
  messages::JobDescriptionReceiverProvider& jdReceiverProvider)
  : m_state(std::make_shared<State>(ioService,
                                    endpoint,
                                    log,
                                    config,
                                    clusterNodeStore,
                                    msgReceiver,
                                    jdReceiverProvider))
{}
TCPAcceptor::~TCPAcceptor() {}

void
TCPAcceptor::startAccepting()
{
  (*this)(boost::system::error_code());
}

#include <boost/asio/yield.hpp>

void
TCPAcceptor::operator()(const boost::system::error_code& ec)
{
  if(!ec) {
    reenter(this)
    {
      if(!newConnection())
        makeNewConnection();

      yield acceptor().async_accept(newConnection()->socket(), *this);

      newConnection()->socket().non_blocking(true);

      newConnection()->readHandler();

      newConnection().reset();
    }
  } else {
    PARACOOBA_LOG(logger(), LocalError)
      << "Error during accepting new connections! Error: " << ec.message();
  }
}

#include <boost/asio/unyield.hpp>

void
TCPAcceptor::makeNewConnection()
{
  newConnection() =
    std::make_unique<Connection>(ioService(),
                                 log(),
                                 config(),
                                 clusterNodeStore(),
                                 messageReceiver(),
                                 jobDescriptionReceiverProvider());
}

}
}

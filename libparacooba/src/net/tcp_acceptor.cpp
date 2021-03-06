#include "../../include/paracooba/net/tcp_acceptor.hpp"
#include "../../include/paracooba/cluster-node-store.hpp"
#include "../../include/paracooba/cluster-node.hpp"
#include "../../include/paracooba/config.hpp"
#include "../../include/paracooba/net/connection.hpp"
#include "../../include/paracooba/networked_node.hpp"
#include <boost/system/error_code.hpp>
#include <limits>
#include <pthread.h>
#include <stdexcept>

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
  , acceptor(ioService, endpoint.protocol())
  , log(log)
  , logger(log->createLogger("TCPAcceptor"))
  , config(config)
  , clusterNodeStore(clusterNodeStore)
  , msgReceiver(msgReceiver)
  , jdReceiverProvider(jdReceiverProvider)
{
  if(config->isTCPAutoPortAssignmentEnabled()) {
    for(auto port = endpoint.port();
        port < std::numeric_limits<decltype(port)>::max();
        ++port) {
      endpoint.port(port);
      config->set(Config::TCPListenPort, port);

      PARACOOBA_LOG(logger, NetTrace)
        << "Automatic TCP port assignment is enabled. Now trying endpoint "
        << endpoint;

      boost::system::error_code ec;
      acceptor.bind(endpoint, ec);
      if(!ec) {
        // Successfully bound!
        acceptor.listen();
        return;
      }

      if(ec == boost::asio::error::address_in_use) {
        PARACOOBA_LOG(logger, NetTrace)
          << "Local endpoint " << endpoint
          << " was already in use! Incrementing port.";
      } else {
        throw std::runtime_error("Cannot bind to endpoint! Error: " +
                                 ec.message());
      }
    }

    throw std::runtime_error("No free TCP port found!");
  } else {
    acceptor.bind(endpoint);
    acceptor.listen();
  }
}

TCPAcceptor::State::~State()
{
  if(newConnection) {
    newConnection->exit();
  }

  PARACOOBA_LOG(logger, Trace)
    << "TCPAcceptor at " << acceptor.local_endpoint() << ":"
    << " stopped.";
}

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
{
  // Check newly known nodes and initialise them if required.
  clusterNodeStore.getNodeFullyKnownSignal().connect(
    std::bind(&TCPAcceptor::initiateConnectionToClusterNode,
              *this,
              std::placeholders::_1));
}
TCPAcceptor::~TCPAcceptor() {}

void
TCPAcceptor::startAccepting()
{
  (*this)(boost::system::error_code());
  PARACOOBA_LOG(logger(), Debug)
    << "TCPAcceptor started at " << acceptor().local_endpoint();
}

#include <boost/asio/yield.hpp>

void
TCPAcceptor::operator()(const boost::system::error_code& ec)
{
  if(!ec) {
    reenter(this)
    {
      for(;;) {
        if(!newConnection())
          makeNewConnection();

        yield acceptor().async_accept(newConnection()->socket(), *this);

        newConnection()->socket().non_blocking(true);

        newConnection()->readHandler();

        newConnection().reset();
      }
    }
  } else {
    PARACOOBA_LOG(logger(), LocalError)
      << "Error during accepting new connections! Error: " << ec.message();
  }
}

#include <boost/asio/unyield.hpp>

void
TCPAcceptor::initiateConnectionToClusterNode(ClusterNode& clusterNode)
{
  if(clusterNode.initializedByPeer()) {
    // Only connections initialised by the local host are tried, so only one
    // side initiates a TCP connection.
    return;
  }

  NetworkedNodePtr nn = clusterNode.getNetworkedNodePtr();
  assert(nn);

  if(nn->isConnectionReady()) {
    // Only initiate connection, if no connection has been made until now. A
    // connection may be already initialised, if TCP is used exclusively.
    return;
  }

  // Start connection.
  Connection newConnectionAttempt(ioService(),
                                  log(),
                                  config(),
                                  clusterNodeStore(),
                                  messageReceiver(),
                                  jobDescriptionReceiverProvider());
  newConnectionAttempt.connect(nn);
}

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

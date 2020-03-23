#include "../../include/paracooba/net/udp_server.hpp"
#include "../../include/paracooba/cluster-node-store.hpp"
#include "../../include/paracooba/cluster-node.hpp"
#include "../../include/paracooba/messages/message.hpp"
#include "../../include/paracooba/networked_node.hpp"
#include <boost/asio/redirect_error.hpp>
#include <cereal/archives/binary.hpp>
#include <mutex>

namespace paracooba {
namespace net {
#define REC_BUF_SIZE 4096

UDPServer::State::State(boost::asio::io_service& ioService,
                        boost::asio::ip::udp::endpoint endpoint,
                        ConfigPtr config,
                        LogPtr log,
                        messages::MessageReceiver& messageReceiver,
                        ClusterNodeStore& clusterNodeStore,
                        ClusterNode& thisNode)
  : socket(ioService, endpoint)
  , config(config)
  , logger(log->createLogger("UDPServer"))
  , messageReceiver(messageReceiver)
  , clusterNodeStore(clusterNodeStore)
  , thisNode(thisNode)
{}

UDPServer::State::~State()
{
  PARACOOBA_LOG(logger, Trace)
    << "UDPServer at " << socket.local_endpoint() << ":"
    << " stopped.";
}

UDPServer::UDPServer(boost::asio::io_service& ioService,
                     boost::asio::ip::udp::endpoint endpoint,
                     ConfigPtr config,
                     LogPtr log,
                     messages::MessageReceiver& messageReceiver,
                     ClusterNodeStore& clusterNodeStore,
                     ClusterNode& thisNode)
  : m_state(std::make_shared<State>(ioService,
                                    endpoint,
                                    config,
                                    log,
                                    messageReceiver,
                                    clusterNodeStore,
                                    thisNode))
{
  NetworkedNode* nn = thisNode.getNetworkedNode();
  assert(nn);
  nn->setRemoteUdpEndpoint(endpoint);
  socket().open(endpoint.protocol());
  socket().set_option(boost::asio::socket_base::broadcast(true));
  socket().bind(endpoint);
  startAccepting();
  PARACOOBA_LOG(logger(), Debug)
    << "UDPServer started at " << socket().local_endpoint();
}
UDPServer::~UDPServer() {}

void
UDPServer::startAccepting()
{
  recvStreambuf().consume(recvStreambuf().size() + 1);
  (*this)(boost::system::error_code(), 0);
}

#include <boost/asio/yield.hpp>

void
UDPServer::operator()(const boost::system::error_code& ec,
                      size_t bytes_received)
{

  if(!ec || ec == boost::asio::error::message_size) {
    reenter(this)
    {
      yield socket().async_receive_from(
        recvStreambuf().prepare(REC_BUF_SIZE), remoteEndpoint(), *this);

      try {
        messages::Message msg;
        recvStreambuf().commit(bytes_received);
        std::istream recvIstream(&recvStreambuf());
        cereal::BinaryInputArchive iarchive(recvIstream);
        iarchive(msg);

        auto [node, inserted] =
          clusterNodeStore().getOrCreateNode(msg.getOrigin());
        NetworkedNode* nn = node.getNetworkedNode();
        nn->setRemoteUdpEndpoint(remoteEndpoint());

        messageReceiver().receiveMessage(msg, *nn);
      } catch(cereal::Exception& e) {
        PARACOOBA_LOG(logger(), GlobalError)
          << "Received invalid message, parsing threw serialisation "
             "exception! "
             "Message: "
          << e.what();
      }

      yield startAccepting();
    }
  } else {
    PARACOOBA_LOG(logger(), LocalError)
      << "Error receiving data from UDP socket. Error: " << ec.message();
    startAccepting();
  }
}

#include <boost/asio/unyield.hpp>

void
UDPServer::transmitMessage(const messages::Message& msg,
                           NetworkedNode& nn,
                           std::function<void(bool)> sendFinishedCB)
{
  assert(nn.isUdpEndpointSet());
  assert(nn.isUdpPortSet());

  std::lock_guard lock(sendMutex());
  std::ostream sendOstream(&sendStreambuf());
  cereal::BinaryOutputArchive oarchive(sendOstream);
  oarchive(msg);

  size_t bytes;
  size_t bytesToSend = sendStreambuf().size();
  bool success = true;

  try {
    bytes = socket().send_to(sendStreambuf().data(), nn.getRemoteUdpEndpoint());
  } catch(const std::exception& e) {
    PARACOOBA_LOG(logger(), LocalError)
      << "Exception encountered when sending message to endpoint "
      << nn.getRemoteUdpEndpoint() << "! Message: " << e.what();
    success = false;
  }
  if(bytes != bytesToSend) {
    PARACOOBA_LOG(logger(), LocalError) << "Only (synchronously) sent " << bytes
                                        << " of target " << bytesToSend << "!";
    success = false;
  }
  sendStreambuf().consume(sendStreambuf().size() + 1);
  sendFinishedCB(success);
}

}
}

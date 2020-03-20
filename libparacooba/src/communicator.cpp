#include "../include/paracooba/communicator.hpp"
#include "../include/paracooba/client.hpp"
#include "../include/paracooba/cnf.hpp"
#include "../include/paracooba/config.hpp"
#include "../include/paracooba/cuber/registry.hpp"
#include "../include/paracooba/daemon.hpp"
#include "../include/paracooba/networked_node.hpp"
#include "../include/paracooba/runner.hpp"

#include "../include/paracooba/messages/message.hpp"
#include "../include/paracooba/messages/node.hpp"
#include "paracooba/messages/announcement_request.hpp"
#include "paracooba/messages/cnftree_node_status_reply.hpp"
#include "paracooba/messages/cnftree_node_status_request.hpp"
#include "paracooba/messages/jobdescription.hpp"
#include "paracooba/messages/offline_announcement.hpp"
#include "paracooba/messages/online_announcement.hpp"

#include <boost/asio.hpp>
#include <boost/asio/buffer.hpp>
#include <boost/asio/deadline_timer.hpp>
#include <boost/asio/high_resolution_timer.hpp>
#include <boost/asio/ip/address.hpp>
#include <boost/asio/ip/address_v4.hpp>
#include <boost/asio/ip/udp.hpp>
#include <boost/asio/socket_base.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/bind.hpp>
#include <boost/date_time/posix_time/posix_time_duration.hpp>
#include <boost/system/error_code.hpp>
#include <cassert>
#include <chrono>
#include <mutex>
#include <netinet/in.h>
#include <regex>
#include <sstream>

#include <cereal/archives/binary.hpp>
#include <stdexcept>

#ifdef ENABLE_INTERNAL_WEBSERVER
#include "../include/paracooba/webserver/api.hpp"
#endif

#define REC_BUF_SIZE 4096

using boost::asio::ip::udp;

namespace paracooba {
void
applyMessageNodeToStatsNode(const messages::Node& src,
                            ClusterStatistics::Node& tgt)
{
  tgt.setName(src.getName());
  tgt.setId(src.getId());
  tgt.setMaximumCPUFrequency(src.getMaximumCPUFrequency());
  tgt.setAvailableWorkers(src.getAvailableWorkers());
  tgt.setUptime(src.getUptime());
  tgt.setWorkQueueCapacity(src.getWorkQueueCapacity());
  tgt.setWorkQueueSize(src.getWorkQueueSize());
  tgt.setUdpListenPort(src.getUdpListenPort());
  tgt.setTcpListenPort(src.getTcpListenPort());
  tgt.setFullyKnown(true);
  tgt.setDaemon(src.getDaemonMode());
}
void
applyMessageNodeStatusToStatsNode(const messages::NodeStatus& src,
                                  ClusterStatistics::Node& tgt,
                                  Config& config)
{
  tgt.setWorkQueueSize(src.getWorkQueueSize());

  if(src.isDaemon()) {
    auto daemon = src.getDaemon();

    for(auto context : daemon.getContexts()) {
      tgt.setContextStateAndSize(
        context.getOriginator(), context.getState(), context.getFactorySize());
    }
  }
}

template<class Socket, typename T>
size_t
writeGenericToSocket(Socket& s, const T& val)
{
  return boost::asio::write(
    s, boost::asio::buffer(reinterpret_cast<const uint8_t*>(&val), sizeof(T)));
}

template<class Streambuf, typename T>
void
readGenericFromStreambuf(Streambuf& b, T& val)
{
  auto buffer = b.data();
  uint8_t* tgt = reinterpret_cast<uint8_t*>(&val);
  std::copy(boost::asio::buffers_begin(buffer),
            boost::asio::buffers_begin(buffer) + sizeof(T),
            tgt);
  b.consume(sizeof(T));
}

class Communicator::UDPServer
{
  public:
  UDPServer(Communicator* comm,
            LogPtr log,
            boost::asio::io_service& ioService,
            uint16_t port)
    : m_communicator(comm)
    , m_socket(ioService)
    , m_logger(log->createLogger("UDPServer"))
    , m_port(port)
    , m_clusterStatistics(comm->getClusterStatistics())
  {
    m_ipAddress = generateIPAddress();
    m_broadcastAddress = generateBroadcastAddress();
    auto endpoint = udp::endpoint(m_ipAddress, port);

    {
      ClusterStatistics::Node& thisNode = m_clusterStatistics->getThisNode();
      assert(!thisNode.getNetworkedNode());
      std::unique_ptr<NetworkedNode> nn =
        std::make_unique<NetworkedNode>(endpoint, thisNode.getId());
      nn->setUdpPort(port);
      thisNode.setNetworkedNode(std::move(nn));
    }

    m_socket.open(endpoint.protocol());
    m_socket.set_option(boost::asio::socket_base::broadcast(true));
    m_socket.bind(endpoint);
    startReceive();
    PARACOOBA_LOG(m_logger, Trace)
      << "UDPServer started at " << m_socket.local_endpoint();
  }
  ~UDPServer()
  {
    PARACOOBA_LOG(m_logger, Trace)
      << "UDPServer at port " << m_port << " stopped.";
  }

  boost::asio::ip::address getIPAddress() { return m_ipAddress; }
  boost::asio::ip::address getBroadcastAddress() { return m_broadcastAddress; }

  void startReceive()
  {
    m_recStreambuf.consume(m_recStreambuf.size() + 1);

    m_socket.async_receive_from(
      m_recStreambuf.prepare(REC_BUF_SIZE),
      m_remoteEndpoint,
      boost::bind(&UDPServer::handleReceive,
                  this,
                  boost::asio::placeholders::error,
                  boost::asio::placeholders::bytes_transferred));
  }

  void handleReceive(const boost::system::error_code& error, std::size_t bytes)
  {
    // No framing required. A single receive always maps directly to a message.
    if(!error || error == boost::asio::error::message_size) {
      messages::Message msg;

      do {
        try {
          m_recStreambuf.commit(bytes);

          std::istream recIstream(&m_recStreambuf);
          cereal::BinaryInputArchive iarchive(recIstream);
          iarchive(msg);
        } catch(cereal::Exception& e) {
          PARACOOBA_LOG(m_logger, GlobalError)
            << "Received invalid message, parsing threw serialisation "
               "exception! "
               "Message: "
            << e.what();
          break;
        }

        switch(msg.getType()) {
          case messages::Type::OnlineAnnouncement:
            PARACOOBA_LOG(m_logger, Trace)
              << "  -> Online Announcement from " << m_remoteEndpoint
              << " (ID: " << msg.getOrigin() << ")";
            handleOnlineAnnouncement(msg);
            break;
          case messages::Type::OfflineAnnouncement:
            PARACOOBA_LOG(m_logger, Trace)
              << "  -> Offline Announcement from " << m_remoteEndpoint
              << " (ID: " << msg.getOrigin() << ")";
            handleOfflineAnnouncement(msg);
            break;
          case messages::Type::AnnouncementRequest:
            PARACOOBA_LOG(m_logger, Trace)
              << "  -> Announcement Request from " << m_remoteEndpoint
              << " (ID: " << msg.getOrigin() << ")";
            handleAnnouncementRequest(msg);
            break;
          case messages::Type::NodeStatus:
            /*
            // These logging messages are not required most of the time and only
            // waste processor time.
            PARACOOBA_LOG(m_logger, Trace)
              << "  -> Node Status from " << m_remoteEndpoint
              << " (ID: " << msg.getOrigin() << ")";
            */
            handleNodeStatus(msg);
            break;
          case messages::Type::CNFTreeNodeStatusRequest:
            PARACOOBA_LOG(m_logger, Trace)
              << "  -> CNFTree Node Status Request from " << m_remoteEndpoint
              << " (ID: " << msg.getOrigin() << ")";
            handleCNFTreeNodeStatusRequest(msg);
            break;
          case messages::Type::CNFTreeNodeStatusReply:
            PARACOOBA_LOG(m_logger, Trace)
              << "  -> CNFTree Node Status Reply from " << m_remoteEndpoint
              << " (ID: " << msg.getOrigin() << ")";
            handleCNFTreeNodeStatusReply(msg);
            break;
          default:
            PARACOOBA_LOG(m_logger, Trace)
              << "  -> Unknown UDP Message from " << m_remoteEndpoint
              << " (ID: " << msg.getOrigin() << ")";
            break;
        }
      } while(false);

      startReceive();
    } else {
      PARACOOBA_LOG(m_logger, LocalError)
        << "Error receiving data from UDP socket. Error: " << error.message();
    }
  }

  void networkStatisticsNode(ClusterStatistics::Node& node)
  {
    NetworkedNode* nn = node.getNetworkedNode();
    if(!nn) {
      std::unique_ptr<NetworkedNode> networkedNode =
        std::make_unique<NetworkedNode>(m_remoteEndpoint, node.getId());
      networkedNode->setUdpPort(m_remoteEndpoint.port());
      networkedNode->setTcpPort(node.getTcpListenPort());
      node.setNetworkedNode(std::move(networkedNode));
      nn = node.getNetworkedNode();
    }

    if(!node.getFullyKnown() &&
       node.getId() != m_communicator->m_config->getInt64(Config::Id)) {
      assert(nn->getRemoteUdpEndpoint().port() != 0);
      m_communicator->m_ioService.post(
        std::bind(&Communicator::task_requestAnnounce,
                  m_communicator,
                  node.getId(),
                  "",
                  nn));
    }
  }

  void sendAnnouncementRequestToKnownPeer(const messages::Node::KnownPeer& peer)
  {
    auto msg = buildMessage();
    msg.insert(messages::AnnouncementRequest());

    boost::asio::ip::udp::endpoint endpoint;
    if(peer.ipAddress[0] == 0) {
      // IPv4 Endpoint
      endpoint = boost::asio::ip::udp::endpoint(
        boost::asio::ip::address_v4(static_cast<uint32_t>(peer.ipAddress[1])),
        peer.port);
    } else {
      // IPv6 Endpoint
      std::array<uint8_t, 16> bytes;
      std::copy(&peer.ipAddress[0], &peer.ipAddress[1], bytes.data());
      endpoint = boost::asio::ip::udp::endpoint(
        boost::asio::ip::address_v6(bytes), peer.port);
    }

    sendMessage(endpoint, msg, false, false);
  }

  void analyseKnownPeersOfNode(const messages::Node& node)
  {
    for(const auto& peer : node.getKnownPeers()) {
      if(messages::Node::peerLocallyReachable(
           m_clusterStatistics->getThisNode().getNetworkedNode(), peer) &&
         !m_clusterStatistics->hasNode(peer.id)) {
        sendAnnouncementRequestToKnownPeer(peer);
      }
    }
  }

  void handleOnlineAnnouncement(const messages::Message& msg)
  {
    const messages::OnlineAnnouncement& onlineAnnouncement =
      msg.getOnlineAnnouncement();
    const messages::Node& messageNode = onlineAnnouncement.getNode();

    int64_t id = msg.getOrigin();

    auto [statisticsNode, inserted] = m_clusterStatistics->getOrCreateNode(id);

    applyMessageNodeToStatsNode(messageNode, statisticsNode);

    networkStatisticsNode(statisticsNode);

    NetworkedNode* nn = statisticsNode.getNetworkedNode();

    // TCP Listen Ports must not change during execution.
    assert((nn->getRemoteTcpEndpoint().port() == 0 ||
            (nn->getRemoteTcpEndpoint().port() ==
             statisticsNode.getTcpListenPort())));

    nn->setTcpPort(statisticsNode.getTcpListenPort());

    if(inserted && !m_communicator->m_config->isDaemonMode()) {
      // Send announcement of this client to the other node before transferring
      // the root CNF.
      m_communicator->m_ioService.post(
        std::bind(&Communicator::task_announce,
                  m_communicator,
                  statisticsNode.getNetworkedNode()));

      if(statisticsNode.isDaemon()) {
        m_communicator->sendCNFToNode(
          m_communicator->m_config->getClient()->getRootCNF(),
          statisticsNode.getId());
      }
    }

    analyseKnownPeersOfNode(messageNode);

    m_communicator->checkAndTransmitClusterStatisticsChanges();
  }
  void handleOfflineAnnouncement(const messages::Message& msg)
  {
    const messages::OfflineAnnouncement& offlineAnnouncement =
      msg.getOfflineAnnouncement();

    m_clusterStatistics->removeNode(msg.getOrigin(),
                                    offlineAnnouncement.getReason());

    m_communicator->checkAndTransmitClusterStatisticsChanges();
  }
  void handleAnnouncementRequest(const messages::Message& msg)
  {
    const messages::AnnouncementRequest& announcementRequest =
      msg.getAnnouncementRequest();
    const messages::Node& requester = announcementRequest.getRequester();

    auto [statisticsNode, inserted] =
      m_clusterStatistics->getOrCreateNode(requester.getId());

    applyMessageNodeToStatsNode(requester, statisticsNode);

    networkStatisticsNode(statisticsNode);

    if(!m_communicator->m_config->isDaemonMode()) {
      if(requester.getDaemonMode()) {
        m_communicator->sendCNFToNode(
          m_communicator->m_config->getClient()->getRootCNF(),
          statisticsNode.getId());
      }
    }

    switch(announcementRequest.getNameMatchType()) {
      case messages::AnnouncementRequest::NameMatch::NO_RESTRICTION:
        break;
      case messages::AnnouncementRequest::NameMatch::REGEX:
        if(!std::regex_match(std::string(m_communicator->m_config->getString(
                               Config::LocalName)),
                             std::regex(announcementRequest.getRegexMatch()))) {
          PARACOOBA_LOG(m_logger, Trace)
            << "No regex match! Regex: " << announcementRequest.getRegexMatch();
          return;
        }
        break;
      case messages::AnnouncementRequest::NameMatch::ID:
        if(m_communicator->m_config->getInt64(Config::Id) !=
           announcementRequest.getIdMatch()) {
          return;
        }
    }

    if(requester.getId() != m_communicator->m_config->getInt64(Config::Id)) {
      m_communicator->m_ioService.post(
        std::bind(&Communicator::task_announce,
                  m_communicator,
                  statisticsNode.getNetworkedNode()));
    }

    analyseKnownPeersOfNode(requester);
  }
  void handleNodeStatus(const messages::Message& msg)
  {
    const messages::NodeStatus& nodeStatus = msg.getNodeStatus();

    int64_t id = msg.getOrigin();

    auto [statisticsNode, inserted] = m_clusterStatistics->getOrCreateNode(id);

    statisticsNode.statusReceived();

    applyMessageNodeStatusToStatsNode(
      nodeStatus, statisticsNode, *m_communicator->m_config);

    networkStatisticsNode(statisticsNode);

    m_communicator->checkAndTransmitClusterStatisticsChanges();
  }

  void handleCNFTreeNodeStatusRequest(const messages::Message& msg)
  {
    const messages::CNFTreeNodeStatusRequest& cnfTreeNodeStatusRequest =
      msg.getCNFTreeNodeStatusRequest();

    int64_t originId = msg.getOrigin();
    int64_t cnfId = cnfTreeNodeStatusRequest.getCnfId();
    CNFTree::Path path = cnfTreeNodeStatusRequest.getPath();

    CNF* cnf = nullptr;
    auto [statisticsNode, inserted] =
      m_clusterStatistics->getOrCreateNode(originId);
    networkStatisticsNode(statisticsNode);

    if(m_communicator->m_config->isDaemonMode()) {
      auto [context, lock] =
        m_communicator->m_config->getDaemon()->getContext(cnfId);
      if(!context) {
        PARACOOBA_LOG(m_logger, GlobalWarning)
          << "CNFTreeNodeStatusRequest received before Context for ID " << cnfId
          << " existed!";
        return;
      }
      cnf = context->getRootCNF().get();
    }

    if(!cnf) {
      PARACOOBA_LOG(m_logger, LocalWarning)
        << "CNFTreeNodeStatusRequest received, but no CNF found for ID "
        << cnfId << "!";
      return;
    }

    m_communicator->requestCNFTreePathInfo(cnfTreeNodeStatusRequest);
  }
  void handleCNFTreeNodeStatusReply(const messages::Message& msg)
  {
    const messages::CNFTreeNodeStatusReply& cnfTreeNodeStatusReply =
      msg.getCNFTreeNodeStatusReply();
    int64_t originId = msg.getOrigin();
    int64_t cnfId = cnfTreeNodeStatusReply.getCnfId();
    uint64_t path = cnfTreeNodeStatusReply.getPath();
    auto& handleStack = cnfTreeNodeStatusReply.getHandleStack();

    // Depending on the handle, this is sent to the internal CNFTree handling
    // mechanism (handle == 0) or to the webserver for viewing (handle != 0).

    auto nodes = cnfTreeNodeStatusReply.getNodes();

    if(nodes.size() == 0) {
      PARACOOBA_LOG(m_logger, GlobalWarning)
        << "Cannot parse CNFTreeNodeStatusReply message for path \""
        << CNFTree::pathToStrNoAlloc(path)
        << "\"! Does not contain any "
           "nodes.";
      return;
    }

    PARACOOBA_LOG(m_logger, Trace)
      << "Receive CNFTree info for path " << CNFTree::pathToStrNoAlloc(path);

    if(handleStack.size() == 1) {
      // This reply arrived at the original sender! Inject all available
      // information.
      for(auto& nodeIt : nodes) {
        m_communicator->injectCNFTreeNodeInfo(
          cnfId,
          handleStack.top(),
          nodeIt.path,
          static_cast<CNFTree::State>(nodeIt.state),
          originId);
      }
    } else {
      // Forward the reply to the next hop. Stack is implicitly popped.
      messages::Message replyMsg;
      messages::CNFTreeNodeStatusReply reply(cnfTreeNodeStatusReply);
      replyMsg.insertCNFTreeNodeStatusReply(std::move(reply));
      sendMessage(cnfTreeNodeStatusReply.getHandle(), replyMsg, false);
    }
  }

  void handleSend(const boost::system::error_code& error,
                  std::size_t bytes,
                  std::mutex* sendBufMutex,
                  boost::asio::streambuf* sendStreambuf,
                  size_t bytesToSend)
  {
    sendBufMutex->unlock();

    sendStreambuf->consume(sendStreambuf->size() + 1);

    if(!error || error == boost::asio::error::message_size) {
      if(bytes != bytesToSend) {
        PARACOOBA_LOG(m_logger, LocalError)
          << "Only (asynchronously) sent " << bytes << " of target "
          << bytesToSend << "!";
      }
    } else {
      PARACOOBA_LOG(m_logger, LocalError)
        << "Error sending data from UDP socket. Error: " << error.message();
    }
  }

  messages::Message buildMessage(int64_t targetId = 0)
  {
    assert(m_communicator);
    assert(m_communicator->m_config);
    return messages::Message(m_communicator->m_config->getInt64(Config::Id),
                             targetId);
  }

  /** @brief Tries to send a message to the given target node (by ID) if that
   * target exists. If it does not exist, this method returns false, else, it
   * returns true. */
  bool sendMessage(int64_t targetNode,
                   const messages::Message& msg,
                   bool fromRunner = true,
                   bool async = false)
  {
    if(!m_clusterStatistics->hasNode(targetNode)) {
      return false;
    }
    NetworkedNode* nn =
      m_clusterStatistics->getNode(targetNode).getNetworkedNode();
    if(!nn) {
      return false;
    }

    return sendMessage(nn->getRemoteUdpEndpoint(), msg, fromRunner, async);
  }

  /** Sends a message to the given endpoint. Always returns true. */
  bool sendMessage(boost::asio::ip::udp::endpoint endpoint,
                   const messages::Message& msg,
                   bool fromRunner = true,
                   bool async = false)
  {
    {
      // Lock the mutex when sending and unlock it when the data has been sent
      // in the handleSend function. This way, async sending is still fast when
      // calculating stuff and does not block, but it DOES block when more
      // messages are sent.
      //
      // This is only required when sending from a runner thread, which is the
      // default. Messages sent from boost asio do not need this synchronisation
      // safe-guard.
      if(fromRunner) {
        m_sendBufMutex.lock();
      }

      {
        std::ostream sendOstream(&m_sendStreambuf);
        cereal::BinaryOutputArchive oarchive(sendOstream);
        oarchive(msg);
      }

      size_t bytesToSend = m_sendStreambuf.size();

      if(async) {
        try {
          m_socket.async_send_to(
            m_sendStreambuf.data(),
            endpoint,
            boost::bind(&UDPServer::handleSend,
                        this,
                        boost::asio::placeholders::error,
                        boost::asio::placeholders::bytes_transferred,
                        &m_sendBufMutex,
                        &m_sendStreambuf,
                        bytesToSend));
        } catch(const std::exception& e) {
          PARACOOBA_LOG(m_logger, LocalError)
            << "Exception encountered when sending async message to endpoint "
            << endpoint << "! Message: " << e.what();
        }
      } else {
        size_t bytes;
        try {
          bytes = m_socket.send_to(m_sendStreambuf.data(), endpoint);
        } catch(const std::exception& e) {
          PARACOOBA_LOG(m_logger, LocalError)
            << "Exception encountered when sending message to endpoint "
            << endpoint << "! Message: " << e.what();
        }
        if(bytes != bytesToSend) {
          PARACOOBA_LOG(m_logger, LocalError)
            << "Only (synchronously) sent " << bytes << " of target "
            << bytesToSend << "!";
        }
        m_sendStreambuf.consume(m_sendStreambuf.size() + 1);

        if(fromRunner) {
          m_sendBufMutex.unlock();
        }
      }
    }
    return true;
  }

  private:
  boost::asio::ip::address generateBroadcastAddress()
  {
    auto ipAddressString = std::string(
      m_communicator->m_config->getString(Config::IPBroadcastAddress));
    boost::system::error_code err;
    auto address = boost::asio::ip::address::from_string(ipAddressString, err);
    if(err) {
      PARACOOBA_LOG(m_logger, LocalError)
        << "Could not parse given IP Broadcast Address \"" << ipAddressString
        << "\". Error: " << err;
      address = boost::asio::ip::address_v4::broadcast();
    }
    return address;
  }

  boost::asio::ip::address generateIPAddress()
  {
    auto ipAddressString =
      std::string(m_communicator->m_config->getString(Config::IPAddress));
    boost::system::error_code err;
    auto address = boost::asio::ip::address::from_string(ipAddressString, err);
    if(err) {
      PARACOOBA_LOG(m_logger, LocalError)
        << "Could not parse given IP Address \"" << ipAddressString
        << "\". Error: " << err;
      address = boost::asio::ip::address_v4::any();
    }
    return address;
  }

  Communicator* m_communicator;
  Logger m_logger;
  uint16_t m_port;

  udp::socket m_socket;
  udp::endpoint m_remoteEndpoint;
  boost::asio::streambuf m_recStreambuf;

  boost::asio::ip::address m_ipAddress;
  boost::asio::ip::address m_broadcastAddress;

  ClusterStatisticsPtr m_clusterStatistics;

  static thread_local boost::asio::streambuf m_sendStreambuf;
  static thread_local std::mutex m_sendBufMutex;
};

thread_local boost::asio::streambuf Communicator::UDPServer::m_sendStreambuf;
thread_local std::mutex Communicator::UDPServer::m_sendBufMutex;

class Communicator::TCPClient : public std::enable_shared_from_this<TCPClient>
{
  public:
  TCPClient(Communicator* comm,
            LogPtr log,
            boost::asio::io_service& ioService,
            int64_t id,
            TCPMode mode,
            std::shared_ptr<CNF> cnf = nullptr)
    : m_comm(comm)
    , m_cnf(cnf)
    , m_mode(mode)
    , m_socket(ioService)
    , m_id(id)
    , m_log(log)
    , m_timer(ioService)
    , m_logger(log->createLoggerMT("TCPClient", "(0)|" + std::to_string(id)))
  {
    //  Receive networked node for specified ID, if it still exists.
    auto& node = m_comm->getClusterStatistics()->getNode(id);
    m_nn = node.getNetworkedNode();
    if(m_nn->deletionRequested()) {
      throw std::invalid_argument("Remote node was requested for deletion!");
    }
    if(m_nn->getRemoteTcpEndpoint().port() == 0) {
      throw std::invalid_argument("NetworkedNode with port 0 encountered!");
    }
    m_nn->addActiveTCPClient();
    m_endpoint = m_nn->getRemoteTcpEndpoint();
    m_logger = log->createLoggerMT("TCPClient",
                                   "(0)|" + std::to_string(m_id) + "@" +
                                     m_endpoint.address().to_string());
  }
  virtual ~TCPClient()
  {
    if(m_nn) {
      m_nn->removeActiveTCPClient();
    }
    PARACOOBA_LOG(m_logger, Trace) << "Destroy TCPClient to " << m_endpoint;
  }

  void insertJobDescription(messages::JobDescription&& jd,
                            std::function<void(bool)> finishedCB)
  {
    m_jobDescription = std::move(jd);
    m_finishedCB = finishedCB;

    m_logger = m_log->createLoggerMT("TCPClient",
                                     m_jobDescription->tagline() + "|" +
                                       m_endpoint.address().to_string());
  }

  void connect()
  {
    PARACOOBA_LOG(m_logger, Trace)
      << "Start TCPClient to " << m_endpoint << " with mode " << m_mode;
    m_socket.async_connect(m_endpoint,
                           boost::bind(&TCPClient::handleConnect,
                                       shared_from_this(),
                                       boost::asio::placeholders::error));
  }

  void handleConnect(const boost::system::error_code& err)
  {
    if(err) {
      PARACOOBA_LOG(m_logger, LocalError)
        << "Could not connect to remote endpoint " << m_endpoint << " (id "
        << m_id << ") over TCP! Error: " << err;

      if(m_jobDescription.has_value()) {
        PARACOOBA_LOG(m_logger, Trace)
          << "This client should transmit a JobDescription, so the client will "
             "be started again.";

        auto ptr = shared_from_this();

        m_timer.expires_from_now(std::chrono::milliseconds(500));
        m_timer.async_wait([ptr, this](const boost::system::error_code& e) {
          if(e != boost::asio::error::operation_aborted) {
            auto jd = std::move(m_jobDescription.value());
            m_jobDescription.reset();
            m_comm->transmitJobDescription(std::move(jd), m_id, m_finishedCB);
          }
        });
      }
      return;
    }

    // Optimizes TCP sending on Linux by waiting for more data to arrive before
    // sending.
    int option = 1;
    setsockopt(m_socket.native_handle(),
               SOL_TCP,
               TCP_CORK,
               (char*)&option,
               sizeof(option));
    m_socket.native_non_blocking(true);

    /// TCP Transmission Protocol specified in \ref tcpcommunication.

    // Sending always starts with the ID of the sender.
    size_t sentBytes =
      writeGenericToSocket(m_socket, m_comm->m_config->getInt64(Config::Id));
    sentBytes += writeGenericToSocket(m_socket, static_cast<uint8_t>(m_mode));

    const size_t expectedSize = sizeof(int64_t) + sizeof(uint8_t);
    if(sentBytes != expectedSize) {
      PARACOOBA_LOG(m_logger, LocalError)
        << "Could not send " << expectedSize << " bytes! Only sent "
        << sentBytes << " bytes.";
      return;
    }

    // The transmission type handling is done inside the CNF sending functions,
    // receiving is therefore completely handled inside the CNF class. Only the
    // ID is handled outside to be able to match the connection to the CNF on
    // the other side.
    switch(m_mode) {
      case TCPMode::TransmitCNF:
        transmitCNF();
        break;
      case TCPMode::TransmitJobDescription:
        transmitJobDescription();
        break;
      default:
        PARACOOBA_LOG(m_logger, LocalError)
          << "Cannot send data, mode " << m_mode << " not handled!";
        break;
    }
  }

  void transmitCNF()
  {
    auto ptr = shared_from_this();

    // Let the sending be handled by the CNF itself.
    m_cnf->send(&m_socket, [this, ptr]() {
      // Finished sending CNF! This should never result in an error and if it
      // would, the remote would never be initialised.
    });
  }

  void transmitJobDescription()
  {
    std::ostream outStream(&m_sendStreambuf);
    cereal::BinaryOutputArchive oa(outStream);
    oa(m_jobDescription.value());

    uint32_t sizeOfArchive = boost::asio::buffer_size(m_sendStreambuf.data());
    if(writeGenericToSocket(m_socket, sizeOfArchive) != sizeof(sizeOfArchive)) {
      PARACOOBA_LOG(m_logger, LocalError)
        << "Could not write size of JD Archive!";
      return;
    }

    PARACOOBA_LOG(m_logger, Trace)
      << "Transmit job description with size " << sizeOfArchive;

    boost::asio::async_write(
      m_socket,
      m_sendStreambuf.data(),
      boost::bind(&TCPClient::transmissionFinished,
                  shared_from_this(),
                  boost::asio::placeholders::error,
                  boost::asio::placeholders::bytes_transferred,
                  sizeOfArchive));
  }

  void transmissionFinished(const boost::system::error_code& error,
                            std::size_t bytes,
                            uint32_t bytesToBeSent)
  {
    if(bytes != bytesToBeSent) {
      PARACOOBA_LOG(m_logger, LocalError)
        << "Bytes sent are not equal to bytes to be sent! " << bytes
        << " != " << bytesToBeSent;
    }

    if(error.value() == boost::system::errc::success) {
      m_finishedCB(true);
    } else {
      PARACOOBA_LOG(m_logger, LocalError)
        << "Transmission failed with error: " << error.message()
        << " - Restarting TCPClient with same body.";

      auto ptr = shared_from_this();

      m_timer.expires_from_now(std::chrono::milliseconds(500));
      m_timer.async_wait([ptr, this](const boost::system::error_code& e) {
        if(e != boost::asio::error::operation_aborted) {
          auto jd = std::move(m_jobDescription.value());
          m_jobDescription.reset();
          m_comm->transmitJobDescription(std::move(jd), m_id, m_finishedCB);
        }
      });
    }
  }

  private:
  boost::asio::streambuf m_sendStreambuf;
  LogPtr m_log;
  LoggerMT m_logger;
  Communicator* m_comm;
  std::shared_ptr<CNF> m_cnf;
  boost::asio::ip::tcp::socket m_socket;
  int64_t m_id;
  NetworkedNode* m_nn = nullptr;
  boost::asio::ip::tcp::endpoint m_endpoint;
  TCPMode m_mode;
  std::optional<messages::JobDescription> m_jobDescription;
  std::function<void(bool)> m_finishedCB;
  boost::asio::high_resolution_timer m_timer;
};

class Communicator::TCPServer
{
  public:
  TCPServer(Communicator* comm,
            LogPtr log,
            boost::asio::io_service& ioService,
            uint16_t port)
    : m_communicator(comm)
    , m_logger(log->createLogger("TCPServer"))
    , m_log(log)
    , m_port(port)
    , m_ioService(ioService)
    , m_acceptor(
        ioService,
        boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), port))
  {
    startAccept();
    PARACOOBA_LOG(m_logger, Trace)
      << "TCPServer started at " << m_acceptor.local_endpoint();
  }
  virtual ~TCPServer()
  {
    PARACOOBA_LOG(m_logger, Trace)
      << "TCPServer at port " << m_port << " stopped.";
  }

  class TCPServerClient : public std::enable_shared_from_this<TCPServerClient>
  {
    public:
    TCPServerClient(boost::asio::io_service& ioService,
                    LogPtr log,
                    Communicator* comm)
      : m_ioService(ioService)
      , m_socket(ioService)
      , m_logger(log->createLogger("TCPServerClient"))
      , m_comm(comm)
    {}
    virtual ~TCPServerClient()
    {
      std::string socketDesc = "Unknown Remote Endpoint (Exit)";
      if(m_socket.is_open()) {
        socketDesc =
          boost::lexical_cast<std::string>(m_socket.remote_endpoint());
      }
      PARACOOBA_LOG(m_logger, Trace)
        << "TCP Connection ended from " << socketDesc;
    }

    boost::asio::ip::tcp::socket& socket() { return m_socket; }

    void start() { handshake(); }

    enum class HandshakePhase
    {
      Start,
      ReadSenderIDAndSubject,
      ReadBody,
      ReadLength,
      ReadJobDescription,
      End
    };

    void handshake(
      boost::system::error_code error = boost::system::error_code(),
      std::size_t bytes = 0,
      HandshakePhase phase = HandshakePhase::Start,
      size_t expectedBytes = 0)
    {
      using namespace boost::asio;
      bool readSome = false;

      switch(phase) {
        case HandshakePhase::Start:
          phase = HandshakePhase::ReadSenderIDAndSubject;
          bytes = sizeof(int64_t) + sizeof(uint8_t);
          break;
        case HandshakePhase::ReadSenderIDAndSubject: {
          m_streambuf.commit(bytes);
          uint8_t modeUint = 0;
          readGenericFromStreambuf(m_streambuf, m_senderID);
          readGenericFromStreambuf(m_streambuf, modeUint);
          m_mode = static_cast<TCPMode>(modeUint);

          if(m_senderID == 0) {
            PARACOOBA_LOG(m_logger, LocalError)
              << "Received id = 0! Invalid ID. Exit TCPServerClient.";
            return;
          }

          switch(m_mode) {
            case TCPMode::TransmitCNF:
              phase = HandshakePhase::ReadBody;
              bytes = REC_BUF_SIZE;
              readSome = true;
              break;
            case TCPMode::TransmitJobDescription:
              phase = HandshakePhase::ReadLength;
              bytes = sizeof(uint32_t);
              break;
            default:
              PARACOOBA_LOG(m_logger, LocalError)
                << "Received unknown TCPMode! Mode: " << m_mode;
              return;
          }
          break;
        }
        case HandshakePhase::ReadBody: {
          m_streambuf.commit(bytes);
          std::stringstream ssOut;
          boost::asio::streambuf::const_buffers_type constBuffer =
            m_streambuf.data();

          std::copy(boost::asio::buffers_begin(constBuffer),
                    boost::asio::buffers_begin(constBuffer) + bytes,
                    std::ostream_iterator<uint8_t>(ssOut));

          m_streambuf.consume(bytes);

          m_cnf->receive(&m_socket, ssOut.str().c_str(), bytes);

          bytes = REC_BUF_SIZE;
          readSome = true;
          break;
        }
        case HandshakePhase::ReadLength: {
          m_streambuf.commit(bytes);
          uint32_t jdSize = 0;
          readGenericFromStreambuf(m_streambuf, jdSize);
          bytes = jdSize;
          phase = HandshakePhase::ReadJobDescription;
          break;
        }
        case HandshakePhase::ReadJobDescription: {
          m_streambuf.commit(bytes);
          if(bytes != expectedBytes) {
            PARACOOBA_LOG(m_logger, LocalError)
              << "Did not read expected " << expectedBytes << " bytes, but "
              << bytes
              << " bytes of Job Description! Error: " << error.message();
            return;
          }
          messages::JobDescription jd;
          try {
            std::istream inStream(&m_streambuf);
            cereal::BinaryInputArchive inAr(inStream);
            inAr(jd);
          } catch(const cereal::Exception& e) {
            PARACOOBA_LOG(m_logger, GlobalError)
              << "Exception during parsing of job description! Error: "
              << e.what();
            return;
          }

          if(m_comm->m_config->isDaemonMode()) {
            auto [context, inserted] =
              m_comm->m_config->getDaemon()->getOrCreateContext(
                jd.getOriginatorID());
            m_context = &context;
            m_cnf = m_context->getRootCNF();
          } else {
            m_cnf = m_comm->m_config->getClient()->getRootCNF();
          }

          NetworkedNode* nn = m_comm->getClusterStatistics()
                                ->getNode(jd.getOriginatorID())
                                .getNetworkedNode();
          assert(nn);

          m_cnf->receiveJobDescription(m_senderID, std::move(jd), *nn);

          if(m_context) {
            switch(jd.getKind()) {
              case messages::JobDescription::Kind::Path:
                if(!m_context->getReadyForWork()) {
                  m_socket.shutdown(
                    boost::asio::ip::tcp::socket::shutdown_both);
                  m_socket.close();
                }
                break;
              case messages::JobDescription::Kind::Result:
                if(!m_context->getReadyForWork()) {
                  m_socket.shutdown(
                    boost::asio::ip::tcp::socket::shutdown_both);
                  m_socket.close();
                } else {
                  m_context->start(Daemon::Context::State::ResultReceived);
                }
                break;
              case messages::JobDescription::Kind::Unknown:
                break;
              case messages::JobDescription::Kind::Initiator:
                m_context->start(Daemon::Context::State::AllowanceMapReceived);
                break;
            }
          }
          return;
        }

        default:
          break;
      }

      if(error == boost::asio::error::eof) {
        if(phase == HandshakePhase::ReadBody) {
          // End the CNF receiving.
          m_cnf->receive(&m_socket, nullptr, 0);
          if(m_context) {
            m_context->start(Daemon::Context::FormulaReceived);
          }
        }
      } else {
        if(error) {
          PARACOOBA_LOG(m_logger, LocalError)
            << "Could read from TCP connection! Error: " << error.message();
          return;
        }

        auto buffer = m_streambuf.prepare(bytes);

        if(readSome) {
          m_socket.async_read_some(std::move(buffer),
                                   boost::bind(&TCPServerClient::handshake,
                                               shared_from_this(),
                                               placeholders::error,
                                               placeholders::bytes_transferred,
                                               phase,
                                               bytes));
        } else {
          async_read(m_socket,
                     std::move(buffer),
                     boost::bind(&TCPServerClient::handshake,
                                 shared_from_this(),
                                 placeholders::error,
                                 placeholders::bytes_transferred,
                                 phase,
                                 bytes));
        }
      }
    }

    std::shared_ptr<CNF> m_cnf;

    boost::asio::io_service& m_ioService;
    boost::asio::ip::tcp::socket m_socket;
    boost::asio::streambuf m_streambuf;
    Logger m_logger;
    Communicator* m_comm;
    Daemon::Context* m_context = nullptr;

    int64_t m_senderID = 0;
    TCPMode m_mode = TCPMode::Unknown;
  };

  void startAccept()
  {
    std::shared_ptr<TCPServerClient> newConn =
      std::make_shared<TCPServerClient>(m_ioService, m_log, m_communicator);

    m_acceptor.async_accept(newConn->socket(),
                            boost::bind(&TCPServer::handleAccept,
                                        this,
                                        newConn,
                                        boost::asio::placeholders::error()));
  }
  void handleAccept(std::shared_ptr<TCPServerClient> newConn,
                    const boost::system::error_code& error)
  {
    if(error) {
      PARACOOBA_LOG(m_logger, LocalError)
        << "Could not accept new TCP connection! Error: " << error.message();
      return;
    }
    newConn->socket().non_blocking(true);
    newConn->start();

    startAccept();
  }

  private:
  Communicator* m_communicator;
  Logger m_logger;
  LogPtr m_log;
  uint16_t m_port;
  boost::asio::ip::tcp::acceptor m_acceptor;
  boost::asio::io_service& m_ioService;
};

Communicator::Communicator(ConfigPtr config, LogPtr log)
  : m_config(config)
  , m_log(log)
  , m_ioServiceWork(m_ioService)
  , m_logger(log->createLogger("Communicator"))
  , m_signalSet(std::make_unique<boost::asio::signal_set>(m_ioService, SIGINT))
  , m_clusterStatistics(std::make_shared<ClusterStatistics>(config, log))
  , m_tickTimer(
      (m_ioService),
      std::chrono::milliseconds(m_config->getUint64(Config::TickMilliseconds)))
{
  m_config->m_communicator = this;
  m_signalSet->async_wait(std::bind(&Communicator::signalHandler,
                                    this,
                                    std::placeholders::_1,
                                    std::placeholders::_2));

  if(config->isInternalWebserverEnabled()) {
    m_webserverInitiator =
      std::make_unique<webserver::Initiator>(config, log, m_ioService);
  }
}

Communicator::~Communicator()
{
  m_config->m_communicator = nullptr;
  exit();
  PARACOOBA_LOG(m_logger, Trace) << "Destruct Communicator.";
}

void
Communicator::run()
{
  using namespace boost::asio;

  // First, init the local node in cluster statistics with all set variables.
  m_clusterStatistics->initLocalNode();

  if(!listenForIncomingUDP(m_config->getUint16(Config::UDPListenPort)))
    return;
  if(!listenForIncomingTCP(m_config->getUint16(Config::TCPListenPort)))
    return;

  if(!m_runner) {
    m_runner = std::make_shared<Runner>(this, m_config, m_log);
  }
  if(!m_runner->isRunning()) {
    m_runner->start();
  }

  m_ioService.post(
    std::bind(&Communicator::task_requestAnnounce, this, 0, "", nullptr));

  if(m_webserverInitiator) {
    m_ioService.post(
      std::bind(&webserver::Initiator::run, m_webserverInitiator.get()));
  }

  // The timer can only be enabled at this stage, after all other required data
  // structures have been initialised. Also, use a warmup time of 2 seconds -
  // the first 5 seconds do not need this, because other work has to be done.
  m_tickTimer.expires_from_now(std::chrono::seconds(2));
  m_tickTimer.async_wait(std::bind(&Communicator::tick, this));

  PARACOOBA_LOG(m_logger, Trace) << "Communicator io_service started.";
  bool ioServiceRunningWithoutException = true;
  while(ioServiceRunningWithoutException) {
    try {
      m_ioService.run();
      ioServiceRunningWithoutException = false;
    } catch(const std::exception& e) {
      PARACOOBA_LOG(m_logger, LocalError)
        << "Exception encountered from ioService! Message: " << e.what();
    }
  }
  PARACOOBA_LOG(m_logger, Trace) << "Communicator io_service ended.";
  m_runner->stop();
}

void
Communicator::exit()
{
  if(m_runner->m_running) {
    auto [nodeMap, lock] = m_clusterStatistics->getNodeMap();
    for(auto& it : nodeMap) {
      auto& node = it.second;
      task_offlineAnnouncement(node.getNetworkedNode());
    }
  }

  // Early stop m_runner, so that threads must not be notified more often if
  // this is called from a runner thread.
  m_runner->m_running = false;

  m_ioService.post([this]() {
    if(m_webserverInitiator) {
      m_webserverInitiator->stop();
      m_webserverInitiator.reset();
    }
    m_runner->stop();

    // Destruct all servers before io Service is stopped.
    m_udpServer.reset();
    m_tcpServer.reset();

    m_ioService.stop();
  });
}

void
Communicator::startRunner()
{
  if(!m_runner) {
    m_runner = std::make_shared<Runner>(this, m_config, m_log);
  }
  m_runner->start();
}

void
Communicator::signalHandler(const boost::system::error_code& error,
                            int signalNumber)
{
  if(signalNumber == SIGINT) {
    PARACOOBA_LOG(m_logger, Trace) << "SIGINT detected.";
    exit();
  }
}

void
Communicator::checkAndTransmitClusterStatisticsChanges(bool force)
{
  if(m_clusterStatistics->clearChanged() || force) {
#ifdef ENABLE_INTERNAL_WEBSERVER
    // Transmit changes to API, so all web-clients can see the new cluster
    // statistics.
    if(m_webserverInitiator) {
      auto api = m_webserverInitiator->getAPI();
      if(api) {
        api->injectClusterStatisticsUpdate(*m_clusterStatistics);
      }
    }
#endif
  }
}

bool
Communicator::listenForIncomingUDP(uint16_t port)
{
  using namespace boost::asio;
  try {
    m_udpServer = std::make_unique<UDPServer>(this, m_log, m_ioService, port);
    return true;
  } catch(std::exception& e) {
    PARACOOBA_LOG(m_logger, LocalError)
      << "Could not initialize server for incoming UDP connections on port "
      << port << "! Error: " << e.what();
    return false;
  }
}

bool
Communicator::listenForIncomingTCP(uint16_t port)
{
  using namespace boost::asio;
  try {
    m_tcpServer = std::make_unique<TCPServer>(this, m_log, m_ioService, port);
    return true;
  } catch(std::exception& e) {
    PARACOOBA_LOG(m_logger, LocalError)
      << "Could not initialize server for incoming TCP connections on port "
      << port << "! Error: " << e.what();
    return false;
  }
}

void
Communicator::sendCNFToNode(std::shared_ptr<CNF> cnf, int64_t targetID)
{
  // This indirection is required to make this work from worker threads.
  m_ioService.post([this, cnf, targetID]() {
    try {
      auto client = std::make_shared<TCPClient>(
        this, m_log, m_ioService, targetID, TCPMode::TransmitCNF, cnf);
      client->connect();
    } catch(const std::exception& e) {
      PARACOOBA_LOG(m_logger, LocalWarning)
        << "TCPClient to " << targetID << " exited! Reason: " << e.what();
    }
  });

  // Also send the allowance map to a CNF, if this transmits the root formula.
  cnf->sendAllowanceMap(targetID, []() {});
}

void
Communicator::transmitJobDescription(messages::JobDescription&& jd,
                                     int64_t targetID,
                                     std::function<void(bool)> sendFinishedCB)
{
  m_ioService.post(
    [this, targetID, jd{ std::move(jd) }, sendFinishedCB]() mutable {
      try {
        auto client = std::make_shared<TCPClient>(
          this, m_log, m_ioService, targetID, TCPMode::TransmitJobDescription);
        client->insertJobDescription(
          std::move(jd), [this, sendFinishedCB](bool success) {
            sendFinishedCB(success);
            if(success) {
              // The auto shutdown timer can be checked against again
              // now, that the
              // TCPClient was sent.
              getRunner()->conditionallySetAutoShutdownTimer();
            }
          });
        client->connect();
      } catch(const std::exception& e) {
        PARACOOBA_LOG(m_logger, LocalWarning)
          << "TCPClient to " << targetID << " exited! Reason: " << e.what();
        sendFinishedCB(false);
      }
    });
}

void
Communicator::injectCNFTreeNodeInfo(int64_t cnfId,
                                    int64_t handle,
                                    CNFTree::Path p,
                                    CNFTree::State state,
                                    int64_t remote)
{
#ifdef ENABLE_INTERNAL_WEBSERVER
  webserver::API* api = m_webserverInitiator->getAPI();
  if(!api) {
    PARACOOBA_LOG(m_logger, LocalWarning)
      << "Cannot inject CNFTreeNodeInfo into uninitialised webserver::API!";
    return;
  }
  api->injectCNFTreeNode(handle, p, state, remote);
#endif
}

void
Communicator::requestCNFTreePathInfo(
  const messages::CNFTreeNodeStatusRequest& request)
{
  std::shared_ptr<CNF> cnf = GetRootCNF(m_config.get(), request.getCnfId());
  if(!cnf)
    return;
  CNFTree& cnfTree = cnf->getCNFTree();

  CNFTree::Path p = request.getPath();

  int64_t targetNode = cnfTree.getOffloadTargetNodeID(p);

  if(targetNode == -1) {
    // No reply possible as the path is not known!
    return;
  } else if(targetNode == 0) {
    // Handled locally, can directly insert local information, if this should be
    // sent to a remote or inserted into local info if requested locally.

    if(request.getHandleStack().size() == 1) {
      // Handle this request locally.
      injectCNFTreeNodeInfo(request.getCnfId(),
                            request.getHandle(),
                            p,
                            cnfTree.getState(p),
                            m_config->getInt64(Config::Id));
    } else {
      // Build answer message.
      messages::Message replyMsg;
      messages::CNFTreeNodeStatusReply reply(m_config->getInt64(Config::Id),
                                             request);
      reply.addNode(request.getPath(), cnfTree.getState(request.getPath()));
      replyMsg.insertCNFTreeNodeStatusReply(std::move(reply));
      m_udpServer->sendMessage(request.getHandle(), replyMsg, false);
    }
  } else {
    messages::Message requestMsg;
    messages::CNFTreeNodeStatusRequest request(m_config->getInt64(Config::Id),
                                               request);
    requestMsg.insert(std::move(request));
    m_udpServer->sendMessage(targetNode, requestMsg, false);
  }
}

void
Communicator::tick()
{
  assert(m_runner);
  assert(m_udpServer);
  assert(m_clusterStatistics);

  m_runner->checkTaskFactories();

  assert(m_clusterStatistics);
  m_clusterStatistics->tick();

  // Update workQueueSize
  auto& thisNode = m_clusterStatistics->getThisNode();
  thisNode.setWorkQueueSize(m_runner->getWorkQueueSize());
  {
    auto [v, lock] = m_runner->getTaskFactories();
    thisNode.applyTaskFactoryVector(v);
  }
  checkAndTransmitClusterStatisticsChanges();

  messages::NodeStatus::OptionalDaemon optionalDaemon = std::nullopt;

  if(m_config->isDaemonMode()) {
    auto daemon = m_config->getDaemon();
    assert(daemon);

    messages::Daemon daemonMsg;

    auto [contextMap, lock] = daemon->getContextMap();

    for(auto& it : contextMap) {
      auto& context = *it.second;
      messages::DaemonContext ctx(context.getOriginatorId(),
                                  static_cast<uint8_t>(context.getState()),
                                  context.getFactoryQueueSize());
      daemonMsg.addContext(std::move(ctx));
    }

    optionalDaemon = daemonMsg;
  } else {
    auto client = m_config->getClient();
    assert(client);
  }

  messages::NodeStatus nodeStatus(m_runner->getWorkQueueSize(), optionalDaemon);
  messages::Message msg = m_udpServer->buildMessage();
  msg.insert(std::move(nodeStatus));

  // Send built message to all other known nodes.
  auto [nodeMap, lock] = m_clusterStatistics->getNodeMap();
  for(auto& it : nodeMap) {
    auto& node = it.second;
    if(node.getId() != m_config->getInt64(Config::Id)) {
      auto nn = node.getNetworkedNode();
      if(node.getFullyKnown() && nn) {
        m_udpServer->sendMessage(nn->getRemoteUdpEndpoint(), msg, false, false);
      }
    }
  }

  m_clusterStatistics->rebalance();

  m_tickTimer.expires_from_now(
    std::chrono::milliseconds(m_config->getUint64(Config::TickMilliseconds)));
  m_tickTimer.async_wait(std::bind(&Communicator::tick, this));
}

void
Communicator::task_requestAnnounce(int64_t id,
                                   std::string regex,
                                   NetworkedNode* nn)
{
  if(!m_udpServer)
    return;

  uint16_t port = m_config->getUint16(Config::UDPTargetPort);

  messages::Message msg = m_udpServer->buildMessage();
  messages::Node node = m_config->buildNode();

  messages::AnnouncementRequest announcementRequest =
    [&]() -> messages::AnnouncementRequest {
    if(id != 0) {
      return messages::AnnouncementRequest(node, id);
    } else if(regex != "") {
      return messages::AnnouncementRequest(node, regex);
    } else {
      return messages::AnnouncementRequest(node);
    }
  }();
  msg.insert(std::move(announcementRequest));

  auto endpoint = [this, nn, port]() {
    if(nn) {
      return nn->getRemoteUdpEndpoint();
    } else {
      return boost::asio::ip::udp::endpoint(m_udpServer->getBroadcastAddress(),
                                            port);
    }
  }();

  PARACOOBA_LOG(m_logger, Trace)
    << "Send announcement request to endpoint " << endpoint;

  m_udpServer->sendMessage(endpoint, msg, false);
}
void
Communicator::task_announce(NetworkedNode* nn)
{
  if(!m_udpServer)
    return;
  PARACOOBA_LOG(m_logger, Trace) << "Send announcement.";

  auto msg = m_udpServer->buildMessage();
  msg.insert(messages::OnlineAnnouncement(m_config->buildNode()));

  if(nn) {
    m_udpServer->sendMessage(nn->getRemoteUdpEndpoint(), msg, false);
  } else {
    m_udpServer->sendMessage(boost::asio::ip::udp::endpoint(
                               m_udpServer->getBroadcastAddress(),
                               m_config->getUint16(Config::UDPTargetPort)),
                             msg,
                             false);
  }
}

void
Communicator::task_offlineAnnouncement(NetworkedNode* nn)
{
  if(!m_udpServer)
    return;
  PARACOOBA_LOG(m_logger, Trace) << "Send offline announcement.";

  auto msg = m_udpServer->buildMessage();
  msg.insert(messages::OfflineAnnouncement("Shutdown"));

  if(nn) {
    m_udpServer->sendMessage(nn->getRemoteUdpEndpoint(), msg, false);
  } else {
    m_udpServer->sendMessage(boost::asio::ip::udp::endpoint(
                               m_udpServer->getBroadcastAddress(),
                               m_config->getUint16(Config::UDPTargetPort)),
                             msg,
                             false,
                             false);
  }
}

std::ostream&
operator<<(std::ostream& o, Communicator::TCPMode mode)
{
  switch(mode) {
    case Communicator::TCPMode::TransmitCNF:
      o << "Transmit CNF";
      break;
    case Communicator::TCPMode::TransmitJobDescription:
      o << "Transmit Job Description";
      break;
    case Communicator::TCPMode::Unknown:
      o << "Unknown";
      break;
  }
  return o;
}
}

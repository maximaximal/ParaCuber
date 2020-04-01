#include "../../include/paracooba/net/control.hpp"
#include "../../include/paracooba/cluster-node-store.hpp"
#include "../../include/paracooba/cluster-node.hpp"
#include "../../include/paracooba/cnftree.hpp"
#include "../../include/paracooba/config.hpp"
#include "../../include/paracooba/daemon.hpp"
#include "../../include/paracooba/messages/message.hpp"
#include "../../include/paracooba/networked_node.hpp"

#include <regex>

namespace paracooba {
namespace net {
Control::Control(ConfigPtr config,
                 LogPtr log,
                 ClusterNodeStore& clusterNodeStore)
  : m_config(config)
  , m_logger(log->createLogger("Control"))
  , m_clusterNodeStore(clusterNodeStore)
{}
Control::~Control() {}

void
Control::receiveMessage(const messages::Message& msg, NetworkedNode& nn)
{
  PARACOOBA_LOG(m_logger, NetTrace)
    << "  -> " << msg.getType() << " from ID " << msg.getOrigin();

  switch(msg.getType()) {
    case messages::Type::OnlineAnnouncement:
      handleOnlineAnnouncement(msg, nn);
      break;
    case messages::Type::OfflineAnnouncement:
      handleOfflineAnnouncement(msg, nn);
      break;
    case messages::Type::AnnouncementRequest:
      handleAnnouncementRequest(msg, nn);
      break;
    case messages::Type::NodeStatus:
      handleNodeStatus(msg, nn);
      break;
    case messages::Type::CNFTreeNodeStatusRequest:
      handleCNFTreeNodeStatusRequest(msg, nn);
      break;
    case messages::Type::CNFTreeNodeStatusReply:
      handleCNFTreeNodeStatusReply(msg, nn);
      break;
    case messages::Type::Unknown:
      // Nothing to do on unknown messages.
      break;
  }
}

void
Control::announceTo(NetworkedNode& nn)
{
  nn.onlineAnnouncement(*m_config, nn);
}

void
Control::requestAnnouncementFrom(NetworkedNode& nn)
{
  nn.announcementRequest(*m_config, nn);
}

void
Control::handleOnlineAnnouncement(const messages::Message& msg,
                                  NetworkedNode& nn)

{
  const messages::OnlineAnnouncement& onlineAnnouncement =
    msg.getOnlineAnnouncement();
  const messages::Node& messageNode = onlineAnnouncement.getNode();

  int64_t id = msg.getOrigin();

  auto [clusterNode, inserted] = m_clusterNodeStore.getOrCreateNode(id);

  clusterNode.applyOnlineAnnouncementMessage(onlineAnnouncement);
}
void
Control::handleOfflineAnnouncement(const messages::Message& msg,
                                   NetworkedNode& nn)
{
  const messages::OfflineAnnouncement& offlineAnnouncement =
    msg.getOfflineAnnouncement();

  m_clusterNodeStore.removeNode(msg.getOrigin(),
                                offlineAnnouncement.getReason());
}
void
Control::handleAnnouncementRequest(const messages::Message& msg,
                                   NetworkedNode& nn)
{
  const messages::AnnouncementRequest& announcementRequest =
    msg.getAnnouncementRequest();
  const messages::Node& requester = announcementRequest.getRequester();

  auto [clusterNode, inserted] =
    m_clusterNodeStore.getOrCreateNode(requester.getId());

  clusterNode.applyAnnouncementRequestMessage(announcementRequest);

  switch(announcementRequest.getNameMatchType()) {
    case messages::AnnouncementRequest::NameMatch::NO_RESTRICTION:
      break;
    case messages::AnnouncementRequest::NameMatch::REGEX:
      if(!std::regex_match(std::string(m_config->getString(Config::LocalName)),
                           std::regex(announcementRequest.getRegexMatch()))) {
        PARACOOBA_LOG(m_logger, Trace)
          << "No regex match! Regex: " << announcementRequest.getRegexMatch();
        return;
      }
      break;
    case messages::AnnouncementRequest::NameMatch::ID:
      if(m_config->getInt64(Config::Id) != announcementRequest.getIdMatch()) {
        return;
      }
  }

  if(requester.getId() != m_config->getInt64(Config::Id)) {
    announceTo(nn);
  }
}
void
Control::handleNodeStatus(const messages::Message& msg, NetworkedNode& nn)
{
  const messages::NodeStatus& nodeStatus = msg.getNodeStatus();

  int64_t id = msg.getOrigin();

  auto [clusterNode, inserted] = m_clusterNodeStore.getOrCreateNode(id);

  clusterNode.applyNodeStatusMessage(nodeStatus);

  if(!clusterNode.getFullyKnown()) {
    // Try the default target port as last hope to get to know the other node.
    NetworkedNode* nn = clusterNode.getNetworkedNode();
    nn->setUdpPort(m_config->getUint16(Config::UDPTargetPort));

    requestAnnouncementFrom(*nn);
  }
}
void
Control::handleCNFTreeNodeStatusRequest(const messages::Message& msg,
                                        NetworkedNode& nn)
{
  const messages::CNFTreeNodeStatusRequest& cnfTreeNodeStatusRequest =
    msg.getCNFTreeNodeStatusRequest();

  int64_t originId = msg.getOrigin();
  int64_t cnfId = cnfTreeNodeStatusRequest.getCnfId();
  Path path = cnfTreeNodeStatusRequest.getPath();

  CNF* cnf = nullptr;
  auto [clusterNode, inserted] = m_clusterNodeStore.getOrCreateNode(originId);

  if(m_config->isDaemonMode()) {
    auto [context, lock] = m_config->getDaemon()->getContext(cnfId);
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
      << "CNFTreeNodeStatusRequest received, but no CNF found for ID " << cnfId
      << "!";
    return;
  }
}
void
Control::handleCNFTreeNodeStatusReply(const messages::Message& msg,
                                      NetworkedNode& nn)
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

  /*
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
  */
}
}
}

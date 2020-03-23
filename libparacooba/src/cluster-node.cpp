#include "../include/paracooba/cluster-node.hpp"
#include "../include/paracooba/daemon.hpp"
#include "../include/paracooba/networked_node.hpp"
#include "../include/paracooba/task_factory.hpp"

#include "../include/paracooba/messages/node.hpp"
#include "../include/paracooba/messages/node_status.hpp"
#include "../include/paracooba/messages/online_announcement.hpp"

namespace paracooba {
static const size_t ClusterNodeWindowSize = 20;

ClusterNode::ClusterNode(
  bool& changed,
  ID thisId,
  ID id,
  messages::MessageTransmitter& statelessMessageTransmitter,
  ClusterNodeStore& clusterNodeStore)
  : m_changed(changed)
  , m_acc_workQueueSize(boost::accumulators::tag::rolling_window::window_size =
                          ClusterNodeWindowSize)
  , m_acc_durationSinceLastStatus(
      boost::accumulators::tag::rolling_window::window_size =
        ClusterNodeWindowSize)
  , m_thisId(thisId)
  , m_id(id)
  , m_networkedNode(
      std::make_unique<NetworkedNode>(id, statelessMessageTransmitter))
  , m_clusterNodeStore(clusterNodeStore)
{
  initMeanDuration(ClusterNodeWindowSize);
}

ClusterNode::ClusterNode(ClusterNode&& o) noexcept
  : m_changed(o.m_changed)
  , m_name(std::move(o.m_name))
  , m_host(std::move(o.m_host))
  , m_networkedNode(std::move(o.m_networkedNode))
  , m_maximumCPUFrequency(o.m_maximumCPUFrequency)
  , m_uptime(o.m_uptime)
  , m_availableWorkers(o.m_availableWorkers)
  , m_workQueueCapacity(o.m_workQueueCapacity)
  , m_workQueueSize(o.m_workQueueSize)
  , m_id(o.m_id)
  , m_fullyKnown(o.m_fullyKnown)
  , m_daemon(o.m_daemon)
  , m_distance(o.m_distance)
  , m_contexts(std::move(o.m_contexts))
  , m_thisId(o.m_thisId)
  , m_acc_workQueueSize(std::move(o.m_acc_workQueueSize))
  , m_acc_durationSinceLastStatus(std::move(o.m_acc_durationSinceLastStatus))
  , m_clusterNodeStore(o.m_clusterNodeStore)
{}
ClusterNode::~ClusterNode() {}

void
ClusterNode::setNetworkedNode(std::unique_ptr<NetworkedNode> networkedNode)
{
  PARACOOBA_CLUSTERNODE_CHANGED(m_networkedNode, std::move(networkedNode))
}

void
ClusterNode::setWorkQueueSize(uint64_t workQueueSize)
{
  PARACOOBA_CLUSTERNODE_CHANGED(m_workQueueSize, workQueueSize)
  m_acc_workQueueSize(workQueueSize);
}
bool
ClusterNode::getReadyForWork(int64_t id) const
{
  if(id == 0)
    id = m_thisId;
  Daemon::Context::State state =
    static_cast<Daemon::Context::State>(getContextState(id));
  return state & Daemon::Context::WaitingForWork;
}
void
ClusterNode::applyTaskFactoryVector(const TaskFactoryVector& v)
{
  for(auto& e : v) {
    setContextSize(e->getOriginId(), e->getSize());
  }
}

void
ClusterNode::applyMessageNode(const messages::Node& node)
{
  bool wasFullyKnown = getFullyKnown();

  {
    setName(node.getName());
    setId(node.getId());
    setMaximumCPUFrequency(node.getMaximumCPUFrequency());
    setAvailableWorkers(node.getAvailableWorkers());
    setUptime(node.getUptime());
    setWorkQueueCapacity(node.getWorkQueueCapacity());
    setWorkQueueSize(node.getWorkQueueSize());
    setUdpListenPort(node.getUdpListenPort());
    setTcpListenPort(node.getTcpListenPort());
    setFullyKnown(true);
    setDaemon(node.getDaemonMode());

    NetworkedNode* nn = getNetworkedNode();
    nn->setUdpPort(getUdpListenPort());
    nn->setTcpPort(getTcpListenPort());
  }

  if(!wasFullyKnown) {
    // ClusterNode is now fully known! This means, a net::Connection will be
    // established as soon as possible.
  }
}
void
ClusterNode::applyOnlineAnnouncementMessage(
  const messages::OnlineAnnouncement& onlineAnnouncement)
{
  applyMessageNode(onlineAnnouncement.getNode());
}
void
ClusterNode::applyNodeStatusMessage(const messages::NodeStatus& nodeStatus)
{
  setWorkQueueSize(nodeStatus.getWorkQueueSize());

  if(nodeStatus.isDaemon()) {
    auto daemon = nodeStatus.getDaemon();

    for(auto context : daemon.getContexts()) {
      setContextStateAndSize(
        context.getOriginator(), context.getState(), context.getFactorySize());
    }
  }
}
}

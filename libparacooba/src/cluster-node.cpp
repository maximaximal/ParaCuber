#include "../include/paracooba/cluster-node.hpp"
#include "../include/paracooba/task_factory.hpp"
#include "../include/paracooba/networked_node.hpp"
#include "../include/paracooba/daemon.hpp"

namespace paracooba {
static const size_t ClusterNodeWindowSize = 20;

ClusterNode::ClusterNode(bool& changed, int64_t thisId, int64_t id)
  : m_changed(changed)
  , m_acc_workQueueSize(boost::accumulators::tag::rolling_window::window_size =
                          ClusterNodeWindowSize)
  , m_acc_durationSinceLastStatus(
      boost::accumulators::tag::rolling_window::window_size =
        ClusterNodeWindowSize)
  , m_thisId(thisId)
  , m_id(id)
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
}

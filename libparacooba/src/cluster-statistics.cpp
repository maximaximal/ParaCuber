#include "../include/paracooba/cluster-statistics.hpp"
#include "../include/paracooba/client.hpp"
#include "../include/paracooba/cnf.hpp"
#include "../include/paracooba/communicator.hpp"
#include "../include/paracooba/config.hpp"
#include "../include/paracooba/daemon.hpp"
#include "../include/paracooba/networked_node.hpp"
#include "../include/paracooba/task.hpp"
#include "../include/paracooba/task_factory.hpp"
#include <algorithm>
#include <boost/iterator/filter_iterator.hpp>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>

namespace paracooba {
ClusterStatistics::ClusterStatistics(ConfigPtr config, LogPtr log)
  : m_config(config)
  , m_logger(log->createLogger("ClusterStatistics"))
{}

ClusterStatistics::~ClusterStatistics() {}

void
ClusterStatistics::initLocalNode()
{
  assert(m_statelessMessageTransmitter);
  ClusterNode thisNode(m_changed,
                       m_config->getInt64(Config::Id),
                       m_config->getInt64(Config::Id),
                       *m_statelessMessageTransmitter,
                       *this);
  thisNode.setDaemon(m_config->isDaemonMode());
  thisNode.setDistance(1);
  thisNode.setFullyKnown(true);
  thisNode.setUptime(0);
  thisNode.setWorkQueueCapacity(m_config->getUint64(Config::WorkQueueCapacity));
  thisNode.setWorkQueueSize(0);
  thisNode.setAvailableWorkers(m_config->getUint32(Config::ThreadCount));
  thisNode.setContextState(m_config->getInt64(Config::Id),
                           Daemon::Context::WaitingForWork);
  thisNode.setTcpListenPort(m_config->getUint16(Config::TCPListenPort));
  thisNode.setUdpListenPort(m_config->getUint16(Config::UDPListenPort));
  thisNode.setName(m_config->getString(Config::LocalName));
  m_thisNode = &addNode(std::move(thisNode));
}

void
ClusterStatistics::setStatelessMessageTransmitter(
  messages::MessageTransmitter& statelessMessageTransmitter)
{
  m_statelessMessageTransmitter = &statelessMessageTransmitter;
}

const ClusterNode&
ClusterStatistics::getNode(ID id) const
{
  auto [map, lock] = getNodeMap();
  auto it = map.find(id);
  if(it == map.end()) {
    throw std::invalid_argument("Could not find node with id " +
                                std::to_string(id) + "!");
  }
  return it->second;
}

ClusterNode&
ClusterStatistics::getNode(ID id)
{
  auto [map, lock] = getNodeMap();
  auto it = map.find(id);
  if(it == map.end()) {
    throw std::invalid_argument("Could not find node with id " +
                                std::to_string(id) + "!");
  }
  return it->second;
}

ClusterNodeStore::ClusterNodeCreationPair
ClusterStatistics::getOrCreateNode(ID id)
{
  assert(m_statelessMessageTransmitter);
  auto [it, inserted] =
    m_nodeMap.emplace(std::pair{ id,
                                 ClusterNode(m_changed,
                                             m_thisNode->getId(),
                                             id,
                                             *m_statelessMessageTransmitter,
                                             *this) });
  return { it->second, inserted };
}

bool
ClusterStatistics::hasNode(ID id) const
{
  auto [map, lock] = getNodeMap();
  return map.count(id) > 0;
}

ClusterNode&
ClusterStatistics::addNode(ClusterNode&& node)
{
  auto [map, lock] = getUniqueNodeMap();
  return map.emplace(node.m_id, std::move(node)).first->second;
}

void
ClusterStatistics::removeNode(int64_t id, const std::string& reason)
{
  auto [map, lock] = getUniqueNodeMap();
  unsafeRemoveNode(id, reason);
}

void
ClusterStatistics::unsafeRemoveNode(int64_t id, const std::string& reason)
{
  assert(m_thisNode);
  if(id == m_thisNode->m_id)
    return;

  auto it = m_nodeMap.find(id);
  if(it != m_nodeMap.end()) {
    auto& node = it->second;

    // Nodes must only be deleted after all TCP Clients are gone.
    NetworkedNode* nn = node.getNetworkedNode();
    if(nn) {
      if(nn->hasActiveTCPClients()) {
        nn->requestDeletion();
        return;
      }
    }

    PARACOOBA_LOG(m_logger, Trace)
      << "Remove cluster statistics node with id: " << id
      << " becase of reason: " << reason;

    it->second.m_nodeOfflineSignal(reason);
    m_nodeMap.erase(id);
  }
}

SharedLockView<ClusterNodeMap&>
ClusterStatistics::getNodeMap()
{
  std::shared_lock lock(m_nodeMapMutex);
  return { m_nodeMap, std::move(lock) };
}
ConstSharedLockView<ClusterNodeMap>
ClusterStatistics::getNodeMap() const
{
  std::shared_lock lock(m_nodeMapMutex);
  return { m_nodeMap, std::move(lock) };
}

UniqueLockView<ClusterNodeMap&>
ClusterStatistics::getUniqueNodeMap()
{
  std::unique_lock lock(m_nodeMapMutex);
  return { m_nodeMap, std::move(lock) };
}

ClusterNode*
ClusterStatistics::getFittestNodeForNewWork(int originator,
                                            const HandledNodesSet& handledNodes,
                                            int64_t rootCNFID)
{
  auto [map, lock] = getNodeMap();

  ClusterNode* target = m_thisNode;

  float min_fitness = std::numeric_limits<float>::max();
  for(auto it = map.begin(); it != map.end(); ++it) {
    auto& n = it->second;
    if(!n.getFullyKnown() || !n.getReadyForWork(rootCNFID) ||
       handledNodes.count(&n) || (!n.isDaemon() && n.getId() != rootCNFID))
      continue;

    float fitness = n.getFitnessForNewAssignment();
    if(fitness < min_fitness) {
      target = &n;
      min_fitness = fitness;
    }
  }

  if(target == nullptr) {
    return nullptr;
  }

  if(target == m_thisNode) {
    // The local compute node is the best one currently, so no rebalancing
    // required!
    return nullptr;
  }

  // No limit for max node utilisation! Just send the node to the best fitting
  // place.
  return target;
}

void
ClusterStatistics::handlePathOnNode(int64_t originator,
                                    ClusterNode& node,
                                    std::shared_ptr<CNF> rootCNF,
                                    const TaskSkeleton& skel)
{
  Communicator* comm = m_config->getCommunicator();
  Path p = skel.p;

  // Local node should be handled externally, without using this function.
  assert(&node != m_thisNode);

  TaskFactory* taskFactory = rootCNF->getTaskFactory();
  assert(taskFactory);
  taskFactory->addExternallyProcessingTask(originator, p, node);

  // This path should be handled on another compute node. This means, the
  // other compute node requires a Cube-Beam from the Communicator class.
  NetworkedNode *nn = node.getNetworkedNode();
  assert(nn);
  rootCNF->sendPath(*nn, skel, []() {});
}

bool
ClusterStatistics::clearChanged()
{
  bool changed = m_changed;
  m_changed = false;
  return changed;
}

void
ClusterStatistics::rebalance(int originator, TaskFactory& factory)
{
  // Rebalancing must be done once for every context.
  size_t remotes = m_nodeMap.size();

  HandledNodesSet handledNodes;

  // Try to rebalance as much as possible, limited by the number of remotes.
  // This should help with initial offloading in large cluster setups.
  for(size_t i = 0; i < remotes && factory.canProduceTask(); ++i) {
    auto mostFitNode = getFittestNodeForNewWork(
      originator, handledNodes, factory.getRootCNF()->getOriginId());
    if(mostFitNode && !mostFitNode->isFullyUtilized() &&
       factory.canProduceTask()) {
      assert(mostFitNode);

      handledNodes.insert(mostFitNode);

      // Three layers make 8 tasks to work on, this should produce enough work
      // for machines with more cores.
      size_t numberOfTasksToSend =
        std::max(1, mostFitNode->getAvailableWorkers() / 8);

      PARACOOBA_LOG(m_logger, Trace)
        << "Rebalance " << numberOfTasksToSend << " tasks for work with origin "
        << originator << " to node " << mostFitNode->getName() << " ("
        << mostFitNode->getId() << ")";

      for(size_t i = 0; i < numberOfTasksToSend && factory.canProduceTask();
          ++i) {
        auto skel = factory.produceTaskSkeleton();
        handlePathOnNode(originator, *mostFitNode, factory.getRootCNF(), skel);
      }
    } else {
      break;
    }
  }
}
void
ClusterStatistics::rebalance()
{
  if(m_config->hasDaemon()) {
    auto [contextMap, lock] = m_config->getDaemon()->getContextMap();
    for(auto& ctx : contextMap) {
      rebalance(ctx.second->getOriginatorId(), *ctx.second->getTaskFactory());
    }
  } else {
    assert(m_config);
    TaskFactory* taskFactory = m_config->getClient()->getTaskFactory();
    if(!taskFactory)
      return;
    rebalance(m_config->getInt64(Config::Id), *taskFactory);
  }
}
void
ClusterStatistics::tick()
{
  using namespace std::literals::chrono_literals;

  auto [map, lock] = getUniqueNodeMap();

  m_thisNode->statusReceived();

  for(auto& it : map) {
    auto& statNode = it.second;
    auto lastStatus = statNode.getDurationSinceLastStatus();
    auto timeout = std::max(statNode.getMeanDurationSinceLastStatus() * 5,
                            std::chrono::duration<double>(10s));
    if(lastStatus > timeout) {
      std::string message = "Last status update was too long ago";
      unsafeRemoveNode(it.first, message);
      return;
    }

    NetworkedNode* nn = statNode.getNetworkedNode();
    if(nn->deletionRequested()) {
      unsafeRemoveNode(statNode.getId(), "Deletion was requested.");
    }
  }
}
}

#ifndef PARACOOBA_CLUSTER_NODE
#define PARACOOBA_CLUSTER_NODE

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>

#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/rolling_mean.hpp>
#include <boost/accumulators/statistics/stats.hpp>
#include <boost/signals2/signal.hpp>

#include "cluster-node-store.hpp"
#include "log.hpp"
#include "types.hpp"
#include "util.hpp"

#define PARACOOBA_CLUSTERNODE_CHANGED(MEMBER, VAR) \
  if(MEMBER != VAR) {                              \
    MEMBER = VAR;                                  \
    m_changed = true;                              \
  }

namespace paracooba {
class NetworkedNode;
class TaskFactory;
class ClusterNodeStore;

namespace messages {
class MessageTransmitter;
class NodeStatus;
class OnlineAnnouncement;
class AnnouncementRequest;
class Node;
}

/** @brief Statistics about the performance of a node in the cluster.
 *
 * These statistics are gathered for every node.
 */
class ClusterNode
{
  public:
  explicit ClusterNode(
    bool& changed,
    ID thisId,
    ID id,
    messages::MessageTransmitter& statelessMessageTransmitter,
    ClusterNodeStore& clusterNodeStore);
  ~ClusterNode();

  ClusterNode(ClusterNode&& o) noexcept;

  struct Context
  {
    uint8_t state = 0;
    uint64_t queueSize = 0;
  };

  using ContextMap = std::map<int64_t, Context>;

  void setName(const std::string& name)
  {
    PARACOOBA_CLUSTERNODE_CHANGED(m_name, name)
  }
  void setName(std::string_view name)
  {
    PARACOOBA_CLUSTERNODE_CHANGED(m_name, name)
  }
  void setHost(const std::string& host)
  {
    PARACOOBA_CLUSTERNODE_CHANGED(m_host, host)
  }
  void setNetworkedNode(std::unique_ptr<NetworkedNode> networkedNode);
  void setMaximumCPUFrequency(uint16_t maximumCPUFrequency)
  {
    PARACOOBA_CLUSTERNODE_CHANGED(m_maximumCPUFrequency, maximumCPUFrequency)
  }
  void setAvailableWorkers(uint16_t availableWorkers)
  {
    PARACOOBA_CLUSTERNODE_CHANGED(m_availableWorkers, availableWorkers);
  }
  void setUptime(uint16_t uptime)
  {
    PARACOOBA_CLUSTERNODE_CHANGED(m_uptime, uptime)
  }
  void setWorkQueueCapacity(uint64_t workQueueCapacity)
  {
    PARACOOBA_CLUSTERNODE_CHANGED(m_workQueueCapacity, workQueueCapacity)
  }
  void setWorkQueueSize(uint64_t workQueueSize);
  void setId(int64_t id) { PARACOOBA_CLUSTERNODE_CHANGED(m_id, id) }
  void setFullyKnown(bool fullyKnown)
  {
    PARACOOBA_CLUSTERNODE_CHANGED(m_fullyKnown, fullyKnown)
  }
  void setUdpListenPort(uint16_t udpListenPort)
  {
    PARACOOBA_CLUSTERNODE_CHANGED(m_udpListenPort, udpListenPort)
  }
  void setTcpListenPort(uint16_t tcpListenPort)
  {
    PARACOOBA_CLUSTERNODE_CHANGED(m_tcpListenPort, tcpListenPort)
  }
  void setDaemon(bool daemon)
  {
    PARACOOBA_CLUSTERNODE_CHANGED(m_daemon, daemon)
  }
  void setDistance(uint8_t distance){ PARACOOBA_CLUSTERNODE_CHANGED(m_distance,
                                                                    distance) }

  NetworkedNode* getNetworkedNode() const
  {
    return m_networkedNode.get();
  }

  std::string_view getName() const { return m_name; }
  int64_t getId() const { return m_id; }
  bool getFullyKnown() const { return m_fullyKnown; }
  /** @brief Get ready for work state.
   *
   * Depends on client or daemon mode. In client mode, this returns the
   * readyness for the client ID. In daemon mode, returns the state for the
   * specified ID.*/
  bool getReadyForWork(int64_t id = 0) const;
  bool isDaemon() const { return m_daemon; }
  uint16_t getUdpListenPort() const { return m_udpListenPort; }
  uint16_t getTcpListenPort() const { return m_tcpListenPort; }
  uint64_t getWorkQueueSize() const { return m_workQueueSize; }
  float getUtilization() const
  {
    return static_cast<float>(m_workQueueSize) /
           static_cast<float>(m_availableWorkers);
  }
  /** @brief Calculate fitness for new assigned work. Lower means fitter.
   */
  float getFitnessForNewAssignment() const
  {
    // TODO: Ping should be considered.
    return getUtilization();
  }
  UniqueLockView<ContextMap&> getContextMap()
  {
    return { m_contexts, std::move(std::unique_lock(m_contextsMutex)) };
  }
  ConstSharedLockView<ContextMap> getContextMapShared() const
  {
    return { m_contexts, std::move(std::shared_lock(m_contextsMutex)) };
  }
  uint8_t getContextState(int64_t id) const
  {
    auto [map, lock] = getContextMapShared();
    auto it = map.find(id);
    if(it == map.end())
      return 0;
    return it->second.state;
  }
  void setContextState(int64_t id, uint8_t state)
  {
    auto [map, lock] = getContextMap();
    PARACOOBA_CLUSTERNODE_CHANGED(map[id].state, state)
  }
  void setContextSize(int64_t id, uint64_t size)
  {
    auto [map, lock] = getContextMap();
    PARACOOBA_CLUSTERNODE_CHANGED(map[id].queueSize, size)
  }
  void setContextStateAndSize(int64_t id, uint8_t state, uint64_t size)
  {
    auto [map, lock] = getContextMap();
    PARACOOBA_CLUSTERNODE_CHANGED(map[id].state, state)
    PARACOOBA_CLUSTERNODE_CHANGED(map[id].queueSize, size)
  }
  uint64_t getAggregatedContextSize() const
  {
    uint64_t size = 0;
    auto [map, lock] = getContextMapShared();
    for(auto& e : map) {
      size += e.second.queueSize;
    }
    return size;
  }

  bool isFullyUtilized() const
  {
    return ((float)m_workQueueSize / (float)m_availableWorkers) > 1.5;
  }

  /** @brief Returns true if this node was created because of an
   * AnnouncementRequest control message. */
  bool initializedByPeer() const { return m_initializedByPeer; }

  size_t getSlotsLeft() const
  {
    return static_cast<uint64_t>(m_availableWorkers) -
           std::min(m_workQueueSize, static_cast<uint64_t>(m_availableWorkers));
  }
  uint16_t getAvailableWorkers() const { return m_availableWorkers; }

  void statusReceived()
  {
    auto now = std::chrono::system_clock::now();
    auto durationSinceLastStatus = now - m_lastStatusReceived;
    m_lastStatusReceived = now;
    m_acc_durationSinceLastStatus(durationSinceLastStatus);
  }

  std::chrono::duration<double> getDurationSinceLastStatus() const
  {
    auto now = std::chrono::system_clock::now();
    return now - m_lastStatusReceived;
  }

  std::chrono::duration<double> getMeanDurationSinceLastStatus() const
  {
    return ::boost::accumulators::rolling_mean(m_acc_durationSinceLastStatus);
  }

  bool statusIsOverdue() const
  {
    return getDurationSinceLastStatus() > getMeanDurationSinceLastStatus() * 10;
  }

  void initMeanDuration(size_t windowSize)
  {
    using namespace std::chrono_literals;
    for(size_t i = 0; i < windowSize; ++i) {
      m_acc_durationSinceLastStatus(5s);
    }
  }

  using TaskFactoryVector = std::vector<TaskFactory*>;
  void applyTaskFactoryVector(const TaskFactoryVector& v);

  using NodeOfflineSignal = boost::signals2::signal<void(const std::string&)>;
  NodeOfflineSignal& getNodeOfflineSignal() { return m_nodeOfflineSignal; }

  void applyMessageNode(const messages::Node& node);
  void applyOnlineAnnouncementMessage(
    const messages::OnlineAnnouncement& onlineAnnouncement);
  void applyAnnouncementRequestMessage(
    const messages::AnnouncementRequest& announcementRequest);
  void applyNodeStatusMessage(const messages::NodeStatus& nodeStatus);

  private:
  friend class ClusterStatistics;

  std::string m_name = "Unknown";
  std::string m_host = "";
  std::unique_ptr<NetworkedNode> m_networkedNode;

  std::chrono::time_point<std::chrono::system_clock> m_lastStatusReceived =
    std::chrono::system_clock::now();

  uint16_t m_maximumCPUFrequency = 0;
  uint16_t m_availableWorkers = 0;
  uint16_t m_udpListenPort = 0;
  uint16_t m_tcpListenPort = 0;
  uint32_t m_uptime = 0;
  uint64_t m_workQueueCapacity = 0;
  uint64_t m_workQueueSize = 0;
  int64_t m_id = 0;
  int64_t m_thisId = 0;
  bool m_fullyKnown = false;
  bool m_daemon = false;
  bool m_initializedByPeer = false;
  uint8_t m_distance = 2;

  NodeOfflineSignal m_nodeOfflineSignal;
  ClusterNodeStore& m_clusterNodeStore;

  mutable std::shared_mutex m_contextsMutex;
  ContextMap m_contexts;

  bool& m_changed;

  // Aggregating
  ::boost::accumulators::accumulator_set<
    uint64_t,
    ::boost::accumulators::stats<::boost::accumulators::tag::rolling_mean>>
    m_acc_workQueueSize;

  ::boost::accumulators::accumulator_set<
    std::chrono::duration<double>,
    ::boost::accumulators::stats<::boost::accumulators::tag::rolling_mean>>
    m_acc_durationSinceLastStatus;

  bool operator=(const ClusterNode& n) { return m_id == n.m_id; }

  friend std::ostream& operator<<(std::ostream& o, const ClusterNode& n)
  {
    o << "\"name\": \"" << n.m_name << "\","
      << "\"id\": " << n.m_id << ","
      << "\"maximumCPUFrequency\": " << n.m_maximumCPUFrequency << ","
      << "\"availableWorkers\": " << n.m_availableWorkers << ","
      << "\"udpListenPort\": " << n.m_udpListenPort << ","
      << "\"uptime\": " << n.m_uptime << ","
      << "\"workQueueCapacity\": " << n.m_workQueueCapacity << ","
      << "\"readyForWork\": " << n.getReadyForWork() << ","
      << "\"fullyKnown\": " << n.m_fullyKnown << ","
      << "\"daemon\": " << n.m_daemon << ","
      << "\"highlighted\": false,"
      << "\"fitnessForNewAssignment\": " << n.getFitnessForNewAssignment()
      << ","
      << "\"aggregatedContextSize\": " << n.getAggregatedContextSize() << ","
      << "\"workQueueSize\": " << n.m_workQueueSize;
    return o;
  }
};
}

#endif

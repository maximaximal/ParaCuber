#ifndef PARACOOBA_CLUSTERSTATISTICS_HPP
#define PARACOOBA_CLUSTERSTATISTICS_HPP

#include <cstdint>
#include <iostream>
#include <memory>
#include <set>
#include <shared_mutex>
#include <string>

#include "cluster-node-store.hpp"
#include "cluster-node.hpp"
#include "cnftree.hpp"
#include "log.hpp"
#include "types.hpp"
#include "util.hpp"

namespace paracooba {
namespace messages {
class MessageTransmitter;
}

class TaskSkeleton;

/** @brief Statistics about the whole cluster, based on which decisions may be
 * made.
 */
class ClusterStatistics : public ClusterNodeStore
{
  public:
  /** @brief Constructor
   */
  ClusterStatistics(ConfigPtr config, LogPtr log);
  /** @brief Destructor.
   */
  ~ClusterStatistics();

  void initLocalNode();

  void setStatelessMessageTransmitter(
    messages::MessageTransmitter& statelessMessageTransmitter);

  using HandledNodesSet = std::set<ClusterNode*>;

  virtual const ClusterNode& getNode(ID id) const;
  virtual ClusterNode& getNode(ID id);
  virtual ClusterNodeCreationPair getOrCreateNode(ID id);
  virtual bool hasNode(ID id) const;
  virtual void removeNode(int64_t id, const std::string& reason);

  friend std::ostream& operator<<(std::ostream& o, const ClusterStatistics& c)
  {
    bool first = true;
    o << "{ \"type\": \"clusterstatistics\", \"ClusterStatistics\": [";
    for(auto& it : c.m_nodeMap) {
      auto& node = it.second;
      if(first) {
        o << "{" << node.dumpToJSON() << "}";
        first = false;
      } else {
        o << ",{" << node.dumpToJSON() << "}";
      }
    }
    o << "]}";
    return o;
  }

  SharedLockView<ClusterNodeMap&> getNodeMap();
  ConstSharedLockView<ClusterNodeMap> getNodeMap() const;
  UniqueLockView<ClusterNodeMap&> getUniqueNodeMap();

  bool clearChanged();

  ClusterNode& getThisNode()
  {
    assert(m_statelessMessageTransmitter);
    return *m_thisNode;
  }

  void handlePathOnNode(int64_t originator,
                        ClusterNode& node,
                        std::shared_ptr<CNF> rootCNF,
                        const TaskSkeleton& skel);

  ClusterNode* getFittestNodeForNewWork(int originator,
                                        const HandledNodesSet& handledNodes,
                                        int64_t rootCNFIDID = 0);

  /** @brief Start rebalancing of work to other nodes.
   *
   * Must be done once for every context on a daemon.
   *
   * Part of @see Rebalancing.
   */
  void rebalance(int originator, TaskFactory& factory);
  /** @brief Start rebalancing of work to other nodes.
   *
   * Automatically calls rebalance(int) with correct context.
   *
   * Part of @see Rebalancing.
   */
  void rebalance();

  /** @brief Check all connections for their timeouts. */
  void tick();

  virtual bool remoteConnectionStringKnown(
    const std::string& remoteConnectionString);

  protected:
  friend class Communicator;
  ClusterNode& addNode(ClusterNode&& node);

  private:
  ClusterNodeMap m_nodeMap;
  ClusterNode* m_thisNode;
  bool m_changed = false;
  messages::MessageTransmitter* m_statelessMessageTransmitter = nullptr;

  void unsafeRemoveNode(int64_t id, const std::string& reason);

  ConfigPtr m_config;
  Logger m_logger;
  mutable std::shared_mutex m_nodeMapMutex;
};
}

#endif

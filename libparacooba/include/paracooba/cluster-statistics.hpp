#ifndef PARACOOBA_CLUSTERSTATISTICS_HPP
#define PARACOOBA_CLUSTERSTATISTICS_HPP

#include <cstdint>
#include <iostream>
#include <memory>
#include <set>
#include <shared_mutex>
#include <string>

#include "cluster-node.hpp"
#include "cnftree.hpp"
#include "log.hpp"
#include "types.hpp"
#include "util.hpp"

namespace paracooba {
class TaskSkeleton;

/** @brief Statistics about the whole cluster, based on which decisions may be
 * made.
 */
class ClusterStatistics
{
  public:
  /** @brief Constructor
   */
  ClusterStatistics(ConfigPtr config, LogPtr log);
  /** @brief Destructor.
   */
  ~ClusterStatistics();

  void initLocalNode();

  using HandledNodesSet = std::set<ClusterNode*>;

  const ClusterNode& getNode(int64_t id) const;
  ClusterNode& getNode(int64_t id);

  std::pair<ClusterNode&, bool> getOrCreateNode(int64_t id);

  friend std::ostream& operator<<(std::ostream& o, const ClusterStatistics& c)
  {
    bool first = true;
    o << "{ \"type\": \"clusterstatistics\", \"ClusterStatistics\": [";
    for(auto& it : c.m_nodeMap) {
      auto& node = it.second;
      if(first) {
        o << "{" << node << "}";
        first = false;
      } else {
        o << ",{" << node << "}";
      }
    }
    o << "]}";
    return o;
  }

  SharedLockView<ClusterNodeMap&> getNodeMap();
  ConstSharedLockView<ClusterNodeMap> getNodeMap() const;
  UniqueLockView<ClusterNodeMap&> getUniqueNodeMap();

  bool clearChanged();

  ClusterNode& getThisNode() { return *m_thisNode; }

  void handlePathOnNode(int64_t originator,
                        ClusterNode& node,
                        std::shared_ptr<CNF> rootCNF,
                        const TaskSkeleton& skel);

  bool hasNode(int64_t id);

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

  protected:
  friend class Communicator;
  ClusterNode& addNode(ClusterNode&& node);
  void removeNode(int64_t id, const std::string& reason);

  private:
  ClusterNodeMap m_nodeMap;
  ClusterNode* m_thisNode;
  bool m_changed = false;

  void unsafeRemoveNode(int64_t id, const std::string& reason);

  ConfigPtr m_config;
  Logger m_logger;
  mutable std::shared_mutex m_nodeMapMutex;
};
}

#endif

#ifndef PARACOOBA_CLUSTER_NODE_STORE
#define PARACOOBA_CLUSTER_NODE_STORE

#include "types.hpp"

namespace paracooba {
class ClusterNode;

class ClusterNodeStore
{
  public:
  using ClusterNodeCreationPair = std::pair<ClusterNode&, bool>;

  virtual const ClusterNode& getNode(int64_t id) const = 0;
  virtual ClusterNode& getNode(int64_t id) = 0;
  virtual ClusterNodeCreationPair getOrCreateNode(ID id) = 0;
  virtual bool hasNode(ID id) = 0;
};
}

#endif

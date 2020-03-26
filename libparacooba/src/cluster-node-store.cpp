#include "../include/paracooba/cluster-node-store.hpp"
#include "../include/paracooba/cluster-node.hpp"
#include "../include/paracooba/net/connection.hpp"
#include "../include/paracooba/networked_node.hpp"
#include "../include/paracooba/readywaiter.hpp"

namespace paracooba {
void
ClusterNodeStore::transmitJobDescription(messages::JobDescription&& jd,
                                         ID id,
                                         SuccessCB sendFinishedCB)
{
  ClusterNode& node = getNode(id);
  NetworkedNode* nn = node.getNetworkedNode();
  assert(nn);

  nn->getConnectionReadyWaiter();
}
}

#ifndef PARACOOBA_MESSAGES_CNFTREE_NODE_STATUS_REPLY
#define PARACOOBA_MESSAGES_CNFTREE_NODE_STATUS_REPLY

#include <cassert>
#include <cstdint>
#include <optional>
#include <vector>

#include <cereal/access.hpp>
#include <cereal/types/vector.hpp>

#include "cnftree_node_status_request.hpp"

namespace paracooba {
namespace messages {
class CNFTreeNodeStatusReply
{
  public:
  using HandleStack = CNFTreeNodeStatusRequest::HandleStack;

  CNFTreeNodeStatusReply() {}
  CNFTreeNodeStatusReply(int64_t localId,
                         const CNFTreeNodeStatusRequest& request)
    : path(request.getPath())
    , cnfId(request.getCnfId())
    , handleStack(request.getHandleStack())
    , remoteId(localId)
  {
    assert(handleStack.size() > 1);
    handleStack.pop();
  }
  CNFTreeNodeStatusReply(const CNFTreeNodeStatusReply& reply)
    : path(reply.getPath())
    , cnfId(reply.getCnfId())
    , handleStack(reply.getHandleStack())
    , nodes(reply.getNodes())
    , remoteId(reply.remoteId)
  {
    assert(handleStack.size() > 1);
    handleStack.pop();
  }
  virtual ~CNFTreeNodeStatusReply() {}

  struct Node
  {
    int64_t path;
    uint8_t state;

    private:
    friend class cereal::access;

    template<class Archive>
    void serialize(Archive& ar)
    {
      ar(path, state);
    }
  };

  using NodeVector = std::vector<Node>;

  uint64_t getPath() const { return path; }
  int64_t getCnfId() const { return cnfId; }
  const NodeVector& getNodes() const { return nodes; }
  const HandleStack& getHandleStack() const { return handleStack; }
  int64_t getHandle() const { return handleStack.top(); }
  int64_t getRemoteId() const { return remoteId; }

  void addNode(int64_t path, uint8_t state)
  {
    nodes.push_back({ path, state });
  }

  size_t getNodeSize() const { return nodes.size(); }

  private:
  friend class cereal::access;

  int64_t remoteId;
  uint64_t path;
  int64_t cnfId;
  NodeVector nodes;
  CNFTreeNodeStatusRequest::HandleStack handleStack;

  template<class Archive>
  void serialize(Archive& ar)
  {
    ar(CEREAL_NVP(remoteId),
       CEREAL_NVP(path),
       CEREAL_NVP(cnfId),
       CEREAL_NVP(nodes),
       CEREAL_NVP(handleStack));
  }
};
}
}

#endif

#ifndef PARACOOBA_CNFTREE_HPP
#define PARACOOBA_CNFTREE_HPP

#include <atomic>
#include <boost/signals2/signal.hpp>
#include <cassert>
#include <cstdint>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <set>

#include "log.hpp"
#include "types.hpp"

namespace paracooba {
class Config;
class CNF;
class NetworkedNode;

/** @brief Implements the CNFTree concept from @ref CNFTree.
 */
class CNFTree
{
  public:
  explicit CNFTree(LogPtr log,
                   CNF& rootCNF,
                   std::shared_ptr<Config> config,
                   int64_t originCNFId);
  ~CNFTree();

  static const Path DefaultUninitiatedPath = std::numeric_limits<Path>::max();

  enum State
  {
    Unvisited,
    Working,
    Solving,
    Split,
    SAT,
    UNSAT,
    Dropped,
    Unknown,
    UnknownPath,
    _STATE_COUNT
  };

  /** @brief A single node on the virtual cubing binary tree.
   */
  struct Node
  {
    State state = Unvisited;

    NetworkedNode* receivedFrom = nullptr;
    NetworkedNode* offloadedTo = nullptr;

    inline bool isOffloaded() const { return offloadedTo != nullptr; }
    inline bool isLocal() const { return offloadedTo == nullptr; }
    inline bool requiresRemoteUpdate() const { return receivedFrom != nullptr; }
  };

  using StateChangedSignal = boost::signals2::signal<void(Path, State)>;
  using NodePtr = std::unique_ptr<Node>;
  using NodeMap = std::map<Path, NodePtr>;

  /** @brief Visitor function for traversing a CNF tree.
   *
   * The CubeVar is the current decision, the uint8_t the depth.
   *
   * Return true to abort traversal, false to continue.
   *
   * using VisitorFunc = std::function<bool(CNFTree::CubeVar, uint8_t, State
   * state, int64_t remote)>;
   */

  /** @brief Visit the tree and call the provided visitor function.
   *
   * @return False if the path is not known, true otherwise.
   */
  template<class Visitor>
  bool visit(Path startPath, Path endPath, Visitor visitor) const
  {
    std::lock_guard lock(m_nodeMapMutex);

    assert(getDepth(endPath) < maxPathDepth);

    for(size_t depth = getDepth(startPath); depth <= getDepth(endPath);
        ++depth) {
      const Node* currentNode = getNode(setDepth(endPath, depth));

      // If no node was found, the visitor does not need to be called! Follow
      // the path until the end.
      if(!currentNode)
        continue;

      if(visitor(setDepth(endPath, depth), *currentNode)) {
        break;
      }
    }
    return true;
  }

  using PathSet = std::set<Path>;

  void dumpTreeToFile(const std::string_view& targetFile);
  void dumpNode(Path p,
                const Node* n,
                PathSet& visitedPaths,
                std::ostream& o,
                bool limitedDump,
                const std::string& parentPath);

  /** @brief Get the state for a given path.
   *
   * @return False if the path does not exist yet, true if assignment was
   * successful.
   */
  State getState(Path p) const;

  inline bool isPathKnown(Path p) const { return getNode(p) != nullptr; }

  /** @brief Only called when locally setting states. */
  void setStateFromLocal(Path p, State state);
  /** @brief Only called when receiving results. */
  void setStateFromRemote(Path p, State state, NetworkedNode& remoteNode);
  /** @brief Only called when receiving paths. */
  void insertNodeFromRemote(Path p, NetworkedNode& remoteNode);
  /** @brief Only called when rebalancing. Does not send anything. */
  void offloadNodeToRemote(Path p, NetworkedNode& networkedNode);
  /** @brief Only called when resetting a node in case of re-adding tasks. */
  void resetNode(Path p);
  /** @brief Get the node a path was offloaded to. If the path is handled
   * locally, returns nullptr. If the path does not exist, returns nullptr. */
  NetworkedNode* getOffloadTargetNetworkedNode(Path p);

  Path getTopmostAvailableParent(Path p) const;

  /** @brief This signal is called when the root state changes.
   */
  StateChangedSignal& getRootStateChangedSignal()
  {
    return m_rootStateChangedSignal;
  }

  static const size_t maxPathDepth;
  static inline uint64_t getPath(Path p)
  {
    return p & (0xFFFFFFFF'FFFFFF00u | 0b11000000u);
  }
  static inline uint8_t getDepth(Path p) { return p & 0b00111111u; }
  static inline uint64_t getShiftedPath(Path p) { return p >> 6u; }
  static inline uint64_t getDepthShiftedPath(Path p)
  {
    return p >> ((sizeof(p) * 8) - getDepth(p));
  }

  static inline Path setPath(Path p, Path path)
  {
    return (p & 0b00111111u) | (path & (0xFFFFFFFF'FFFFFF00u | 0b11000000u));
  }

  static inline Path setDepth(Path p, uint8_t d)
  {
    assert(d <= maxPathDepth);
    return getPath(p) | (d & 0b00111111u);
  }

  static inline Path getParent(Path p)
  {
    assert(getDepth(p) >= 1);
    return setDepth(p, getDepth(p) - 1);
  }

  static void pathToStr(Path p, char* str);
  static const char* pathToStrNoAlloc(Path p);
  static std::string pathToStdString(Path p);
  static Path strToPath(const char* str, size_t len);

  template<typename T>
  static inline Path buildPath(T p, uint8_t d)
  {
    Path path = p;
    return setDepth(path << (((sizeof(Path)) - (sizeof(T))) * 8), d);
  }

  static inline bool getAssignment(Path p, uint8_t depth)
  {
    // The depth starts counting with 1 and the sizeof() is 1 too large.
    assert(depth <= maxPathDepth);
    return p &
           (static_cast<Path>(1u) << ((sizeof(Path) * 8) - 1 - (depth - 1)));
  }

  static inline Path setAssignment(Path p, uint8_t depth, bool val)
  {
    // The depth starts counting with 1 and the sizeof() is 1 too large.
    assert(depth >= 1);
    assert(depth <= maxPathDepth);
    return (p & ~(static_cast<Path>(1u)
                  << ((sizeof(Path) * 8) - 1 - (depth - 1)))) |
           (static_cast<Path>(val) << ((sizeof(Path) * 8) - 1 - (depth - 1)));
  }

  static inline Path getSiblingPath(Path p)
  {
    return setAssignment(p, getDepth(p), !getAssignment(p, getDepth(p)));
  }

  static inline Path getNextLeftPath(Path p)
  {
    uint8_t depth = getDepth(p) + 1;
    return setDepth(setAssignment(p, depth, false), depth);
  }
  static inline Path getNextRightPath(Path p)
  {
    uint8_t depth = getDepth(p) + 1;
    return setDepth(setAssignment(p, depth, true), depth);
  }

  static inline Path down(Path p) { return setDepth(p, getDepth(p) + 1); }
  static inline Path up(Path p) { return setDepth(p, getDepth(p) - 1); }

  static inline Path cleanupPath(Path p)
  {
    return setDepth((p & ~(0xFFFFFFFF'FFFFFFFFu >> getDepth(p))), getDepth(p));
  }

  private:
  const Node* getNode(Path p) const
  {
    const auto node = m_nodeMap.find(cleanupPath(p));
    if(node != m_nodeMap.end()) {
      return node->second.get();
    }
    return nullptr;
  }
  Node* getNode(Path p)
  {
    auto node = m_nodeMap.find(cleanupPath(p));
    if(node != m_nodeMap.end()) {
      return node->second.get();
    }
    return nullptr;
  }
  Node* getRootNode() { return getNode(0); }
  const Node* getRootNode() const { return getNode(0); }
  Path getTopmostAvailableParentInner(Path p) const;

  CNF& m_rootCNF;
  std::shared_ptr<Config> m_config;
  int64_t m_originCNFId;
  StateChangedSignal m_rootStateChangedSignal;
  NodeMap m_nodeMap;
  Logger m_logger;

  mutable std::mutex m_nodeMapMutex;
  mutable std::mutex m_logMutex;

  void setCNFResult(Path p, State state, Path source);

  void propagateUpwardsFrom(Path p, Path sourcePath = DefaultUninitiatedPath);

  void sendNodeResultToSender(Path p, const Node& node);
};

constexpr const char*
CNFTreeStateToStr(CNFTree::State s)
{
  switch(s) {
    case CNFTree::State::Unvisited:
      return "Unvisited";
    case CNFTree::State::Working:
      return "Working";
    case CNFTree::State::SAT:
      return "SAT";
    case CNFTree::State::UNSAT:
      return "UNSAT";
    case CNFTree::State::Solving:
      return "Solving";
    case CNFTree::State::Split:
      return "Split";
    case CNFTree::State::Dropped:
      return "Dropped";
    case CNFTree::State::Unknown:
      return "Unknown";
    case CNFTree::State::UnknownPath:
      return "Unknown Path";
    default:
      return "Unknown State";
  }
}

std::ostream&
operator<<(std::ostream& o, CNFTree::State s);

template<>
inline Path
CNFTree::buildPath(uint64_t p, uint8_t d)
{
  return setDepth(p, d);
}

}

#endif

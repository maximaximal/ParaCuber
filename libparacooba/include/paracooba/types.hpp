#ifndef PARACOOBA_TYPES
#define PARACOOBA_TYPES

#include <functional>
#include <map>
#include <optional>
#include <vector>

namespace paracooba {
using Literal = int;
using LiteralMap = std::vector<Literal>;
using Cube = LiteralMap;
using OptionalCube = std::optional<Cube>;

using ID = int64_t;
using Path = uint64_t;

class ClusterNode;
using ClusterNodeMap = std::map<ID, ClusterNode>;

using SuccessCB = std::function<void(bool)>;
static SuccessCB EmptySuccessCB = nullptr;
}

#endif

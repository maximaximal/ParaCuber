#include "../../include/paracooba/cuber/pregenerated.hpp"
#include "../../include/paracooba/cadical_task.hpp"
#include "../../include/paracooba/cnf.hpp"
#include <cassert>
#include <vector>

namespace paracooba {
namespace cuber {
Pregenerated::Pregenerated(ConfigPtr config,
                           LogPtr log,
                           CNF& rootCNF,
                           const messages::JobInitiator& ji)
  : Cuber(config, log, rootCNF)
  , m_ji(ji)
{}

Pregenerated::~Pregenerated() {}

bool
Pregenerated::init()
{
  auto rootTask = m_rootCNF.getRootTask();
  const auto& flatCubeArr = rootTask->getPregeneratedCubes();

  assert(flatCubeArr.size() > 0);

  m_cubesFlatVector = &flatCubeArr;
  m_cubesJumpList.clear();

  size_t i = 0, cube = 0, lastStart = 0;
  for(int lit : flatCubeArr) {
    if(lit == 0) {
      m_cubesJumpList.push_back(lastStart);
      lastStart = i + 1;
    }
    ++i;
  }
  // Last entry for faster copy - this is invalid and only serves to know the
  // lengths of cubes.
  m_cubesJumpList.push_back(lastStart);

  float log = std::log2f(m_cubesJumpList.size() - 1);
  m_normalizedPathLength = std::ceil(log);
  PARACOOBA_LOG(m_logger, Trace)
    << "Initialised " << m_cubesJumpList.size() - 1 << " pregenerated Cubes";
  return true;
}

bool
Pregenerated::shouldGenerateTreeSplit(Path path)
{
  size_t depth = CNFTree::getDepth(path);
  size_t possibleSolvers = 1u << depth;
  return possibleSolvers < (m_cubesJumpList.size() - 1);
}
bool
Pregenerated::getCube(Path path, std::vector<int>& literals)
{
  // This happens when the tree has not come to decisions yet.
  if(CNFTree::getDepth(path) != m_normalizedPathLength)
    return true;

  Path depthShifted = CNFTree::getDepthShiftedPath(
    CNFTree::setDepth(path, m_normalizedPathLength));

  if(depthShifted >= m_cubesJumpList.size() - 1) {
    return false;
  }

  auto begin = m_cubesFlatVector->begin() + m_cubesJumpList[depthShifted];
  auto end =
    m_cubesFlatVector->begin() + (m_cubesJumpList[depthShifted + 1] - 1);
  auto length = end - begin;
  literals.clear();
  literals.reserve(length);

  std::copy(begin, end, std::back_inserter(literals));

  /*
  std::string cubeStr;
  for(int lit : literals) {
    cubeStr.append(std::to_string(lit));
    cubeStr.append(", ");
  }
  PARACOOBA_LOG(m_logger, Trace) << "USING CUBE " << cubeStr;
  */

  return true;
}

}
}

#include "../include/paracooba/decision_task.hpp"
#include "../include/paracooba/cadical_task.hpp"
#include "../include/paracooba/cluster-statistics.hpp"
#include "../include/paracooba/cnf.hpp"
#include "../include/paracooba/communicator.hpp"
#include "../include/paracooba/config.hpp"
#include "../include/paracooba/cuber/registry.hpp"
#include "../include/paracooba/runner.hpp"
#include "../include/paracooba/task_factory.hpp"
#include <cassert>

namespace paracooba {
DecisionTask::DecisionTask(std::shared_ptr<CNF> rootCNF,
                           Path p,
                           const OptionalCube& optionalCube)
  : m_rootCNF(rootCNF)
  , m_path(p)
  , m_optionalCube(optionalCube)
{
  m_name = "DecisionTask for Path " + CNFTree::pathToStdString(p);
}
DecisionTask::~DecisionTask() {}

TaskResultPtr
DecisionTask::execute()
{
  /// Steps to take:
  ///   1. Get the cuber::Registry object.
  ///   2. Ask whatever algorithm is active for next decision.
  ///   3. Determine success.
  ///   4. Build new solver task immediately if the decision was negative and
  ///   submit it to local runner, or send the new path to wherever it fits.
  assert(m_rootCNF->readyToBeStarted());
  assert(m_factory);// This task requires to be run with a valid factory!

  auto& cnfTree = m_rootCNF->getCNFTree();
  auto clusterStatistics = m_config->getCommunicator()->getClusterStatistics();

  CNFTree::State state = cnfTree.getState(m_path);
  if(state != CNFTree::Unvisited && state != CNFTree::UnknownPath) {
    PARACOOBA_LOG((*m_logger), GlobalWarning)
      << "Processing decision task on path "
      << CNFTree::pathToStrNoAlloc(m_path)
      << " that is not completely fresh and was visited before! State of node "
         "on this path: "
      << state << ". Ignoring this warning.";
  }

  cnfTree.setStateFromLocal(m_path, CNFTree::Working);

  if(!m_optionalCube.has_value() &&
     m_rootCNF->getCuberRegistry().shouldGenerateTreeSplit(m_path)) {
    // New split generated! This means, the TRUE and FALSE branches are now
    // available and the generated decision must be set into the path.
    cnfTree.setStateFromLocal(m_path, CNFTree::Split);

    auto& cuberRegistry = m_rootCNF->getCuberRegistry();
    std::vector<int> testLiterals;

    // Both paths are added to the factory, the rebalance mechanism takes care
    // of distribution to other compute nodes.
    {
      // LEFT
      Path p = CNFTree::getNextLeftPath(m_path);
      if(cuberRegistry.getCube(p, testLiterals)) {
        m_factory->addPath(p, TaskFactory::CubeOrSolve, m_originator);
      } else {
        cnfTree.setStateFromLocal(p, CNFTree::UNSAT);
      }
    }
    {
      // RIGHT
      Path p = CNFTree::getNextRightPath(m_path);
      if(cuberRegistry.getCube(p, testLiterals)) {
        m_factory->addPath(p, TaskFactory::CubeOrSolve, m_originator);
      } else {
        cnfTree.setStateFromLocal(p, CNFTree::UNSAT);
      }
    }

    return std::make_unique<TaskResult>(TaskResult::DecisionMade);
  } else {
    PARACOOBA_LOG(*m_logger, Trace)
      << "No decision made for path " << CNFTree::pathToStrNoAlloc(m_path);

    m_factory->addPath(
      m_path, TaskFactory::Solve, m_originator, m_optionalCube);

    return std::make_unique<TaskResult>(TaskResult::NoDecisionMade);
  }
}
}

#ifndef PARACOOBA_DECISIONTASK_HPP
#define PARACOOBA_DECISIONTASK_HPP

#include "cnftree.hpp"
#include "task.hpp"
#include "types.hpp"
#include <memory>

namespace paracooba {
class CNF;

/** @brief The decision task determines the next decision literal.
 *
 * This class should only be instantiated if the decision shall be carried out
 * on the local machine. Remote decisions are decided on remote machines, after
 * beaming a path to another compute node.
 */
class DecisionTask : public Task
{
  public:
  DecisionTask(std::shared_ptr<CNF> rootCNF,
               Path p,
               const OptionalCube& optionalCube);
  virtual ~DecisionTask();

  virtual TaskResultPtr execute();

  Path getPath() const { return m_path; }
  const OptionalCube& getOptionalCube() const { return m_optionalCube; }

  private:
  std::shared_ptr<CNF> m_rootCNF;
  Path m_path;
  OptionalCube m_optionalCube;
};
}

#endif

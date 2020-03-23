#ifndef PARACOOBA_CLIENT_HPP
#define PARACOOBA_CLIENT_HPP

#include <memory>
#include <string_view>

#include "log.hpp"
#include "taskresult.hpp"

#include "messages/jobdescription_receiver.hpp"

namespace paracooba {
class Config;
class Communicator;
class CNF;
class TaskFactory;

/** @brief Main interaction point with the solver as a user.
 */
class Client : public messages::JobDescriptionReceiverProvider
{
  public:
  /** @brief Constructor
   */
  Client(ConfigPtr config,
         LogPtr log,
         std::shared_ptr<Communicator> communicator);
  /** @brief Destructor.
   */
  virtual ~Client();

  /** @brief Read DIMACS file into the internal solver instance, get path from
   * config. */
  std::string_view getDIMACSSourcePathFromConfig();

  /** @brief Try to solve the current formula.
   *
   * @return status code,
   *         - TaskResult::Status::Unsolved
   *         - TaskResult::Status::Satisfiable
   *         - TaskResult::Status::Unsatisfiable
   */
  void solve();
  inline TaskResult::Status getStatus() const { return m_status; }

  std::shared_ptr<CNF> getRootCNF() const { return m_rootCNF; }

  uint32_t getCNFVarCount() const { return m_cnfVarCount; }

  TaskFactory* getTaskFactory() { return m_taskFactory.get(); }

  std::shared_ptr<std::vector<uint8_t>> getSatAssignment()
  {
    return m_satAssignment;
  };

  virtual messages::JobDescriptionReceiver* getJobDescriptionReceiver(
    int64_t subject);

  private:
  ConfigPtr m_config;
  LogPtr m_log;
  Logger m_logger;
  std::shared_ptr<Communicator> m_communicator;
  TaskResult::Status m_status = TaskResult::Unknown;
  std::shared_ptr<CNF> m_rootCNF;
  uint32_t m_cnfVarCount = 0;
  std::unique_ptr<TaskFactory> m_taskFactory;
  std::shared_ptr<std::vector<uint8_t>> m_satAssignment;
};
}

#endif

#include "../include/paracooba/client.hpp"
#include "../include/paracooba/cadical_task.hpp"
#include "../include/paracooba/cnf.hpp"
#include "../include/paracooba/communicator.hpp"
#include "../include/paracooba/config.hpp"
#include "../include/paracooba/cuber/registry.hpp"
#include "../include/paracooba/daemon.hpp"
#include "../include/paracooba/net/connection.hpp"
#include "../include/paracooba/networked_node.hpp"
#include "../include/paracooba/readywaiter.hpp"
#include "../include/paracooba/runner.hpp"
#include "../include/paracooba/task_factory.hpp"

namespace paracooba {
Client::Client(ConfigPtr config, LogPtr log, CommunicatorPtr communicator)
  : m_config(config)
  , m_log(log)
  , m_logger(log->createLogger("Client"))
  , m_communicator(communicator)
{
  m_config->m_client = this;
  m_rootCNF = std::make_shared<CNF>(config,
                                    log,
                                    config->getInt64(Config::Id),
                                    *communicator->getClusterStatistics(),
                                    getDIMACSSourcePathFromConfig());

  // Connect to node fully known signal, so fully known nodes receive the CNF
  // from this client.
  m_communicator->getClusterStatistics()->getNodeFullyKnownSignal().connect(
    [this](const ClusterNode& node) {
      PARACOOBA_LOG(m_logger, Trace)
        << "Node " << node.getId()
        << " fully known event received, transfer CNF.";
      NetworkedNode* nn = node.getNetworkedNode();
      assert(nn);

      nn->getConnectionReadyWaiter().callWhenReady(
        [this, &node, nn](net::Connection& conn) {
          conn.sendCNF(m_rootCNF, [this, &node, &conn, nn](bool success) {
            PARACOOBA_LOG(m_logger, Trace)
              << "Sent root CNF to " << node.getId() << " with "
              << (success ? "success" : "error");

            if(success) {
              // Now, send the allowance map to fully initiate the remote
              // compute node and begin working on tasks.
              m_rootCNF->sendAllowanceMap(*nn, [this, &node]() {
                PARACOOBA_LOG(m_logger, Trace)
                  << "Sent allowance map to " << node.getId() << ".";
              });
            }
          });
        });
    });
}
Client::~Client()
{
  m_config->m_client = nullptr;
}

std::string_view
Client::getDIMACSSourcePathFromConfig()
{
  std::string_view sourcePath = m_config->getString(Config::InputFile);
  if(sourcePath == "") {
    static const char* errorMsg = "No input file given!";
    PARACOOBA_LOG(m_logger, LocalWarning) << errorMsg;
    return errorMsg;
  }
  return sourcePath;
}

void
Client::solve()
{
  CaDiCaLTask::Mode mode = CaDiCaLTask::Parse;

  // Result found signal.
  m_rootCNF->getResultFoundSignal().connect([this](CNF::Result* result) {
    PARACOOBA_LOG(m_logger, Trace)
      << "Received result in Client, exit Communicator. Result: "
      << result->state;

    switch(result->state) {
      case CNFTree::SAT:
        m_status = TaskResult::Satisfiable;

        if(!result->decodedAssignment)
          result->decodeAssignment();

        assert(result->decodedAssignment);
        m_satAssignment = result->decodedAssignment;
        PARACOOBA_LOG(m_logger, Trace)
          << "Satisfying assignment available with "
          << result->decodedAssignment->size() << " literals ("
          << BytePrettyPrint(result->decodedAssignment->size() *
                             sizeof(Literal)) << " in memory)";
        break;
      case CNFTree::UNSAT:
        m_status = TaskResult::Unsatisfiable;
        break;
      default:
        m_status = TaskResult::Unsolved;
        break;
    }
    m_communicator->exit();
  });

  auto task = std::make_unique<CaDiCaLTask>(&m_cnfVarCount, mode);
  task->setRootCNF(m_rootCNF);
  auto& finishedSignal = task->getFinishedSignal();
  task->readDIMACSFile(getDIMACSSourcePathFromConfig());
  m_communicator->getRunner()->push(std::move(task),
                                    m_config->getInt64(Config::Id));
  finishedSignal.connect([this](const TaskResult& result) {
    if(m_status == TaskResult::Unknown) {
      m_status = result.getStatus();
    }
    if(m_status != TaskResult::Parsed) {
      PARACOOBA_LOG(m_logger, Fatal)
        << "Could not parse DIMACS source file! Status: " << result.getStatus()
        << ". Exiting Client.";
      m_communicator->exit();
      return;
    }

    // Formula has been parsed successfully
    // The internal CNF can therefore now be fully initialised with this root
    // CNF solver task. The root CNF now has a valid unique_ptr to a completely
    // parsed CaDiCaL task.
    // This is (in theory) unsafe, but should only be required in this case. It
    // should not be required to const cast the result anywhere else, except in
    // daemon.
    auto& resultMut = const_cast<TaskResult&>(result);
    {
      auto rootTaskMutPtr = static_unique_pointer_cast<CaDiCaLTask>(
        std::move(resultMut.getTaskPtr()));
      assert(rootTaskMutPtr);
      m_rootCNF->setRootTask(std::move(rootTaskMutPtr));
    }

    // Client is now ready for work.
    m_config->m_communicator->getClusterStatistics()
      ->getThisNode()
      .setContextState(m_config->getInt64(Config::Id),
                       Daemon::Context::WaitingForWork);

    // After the root formula has been set, which completely builds up the CNF
    // object including the cuber::Registry object, decisions can be made.
    // Decisions must be propagated to daemons in order to solve them in
    // parallel.
    m_taskFactory = std::make_unique<TaskFactory>(m_config, m_log, m_rootCNF);
    m_taskFactory->setRootTask(m_rootCNF->getRootTask());
    m_rootCNF->setTaskFactory(m_taskFactory.get());

    // If local solving is enabled, create a new task to solve the parsed
    // formula.
    if(m_config->isClientCaDiCaLEnabled()) {
      auto task = std::make_unique<CaDiCaLTask>(
        static_cast<CaDiCaLTask&>(*m_rootCNF->getRootTask()));
      task->setMode(CaDiCaLTask::Solve);
      task->setCaDiCaLMgr(m_taskFactory->getCaDiCaLMgr());
      auto& finishedSignal = task->getFinishedSignal();
      task->readDIMACSFile(getDIMACSSourcePathFromConfig());
      m_communicator->getRunner()->push(std::move(task),
                                        m_config->getInt64(Config::Id));
      finishedSignal.connect([this](const TaskResult& result) {
        if(m_status == TaskResult::Unknown || m_status == TaskResult::Parsed) {
          m_status = result.getStatus();
          PARACOOBA_LOG(m_logger, Trace)
            << "Solution found via client solver, this was faster than CnC.";
          // Finished solving using client CaDiCaL!
          m_communicator->exit();
        }
      });
    }

    m_taskFactory->addPath(
      0, TaskFactory::CubeOrSolve, m_config->getInt64(Config::Id));
  });
}

messages::JobDescriptionReceiver*
Client::getJobDescriptionReceiver(int64_t subject)
{
  assert(m_rootCNF);

  if(subject != m_rootCNF->getOriginId()) {
    PARACOOBA_LOG(m_logger, LocalError)
      << "Requested subject with ID " << subject
      << " does not match only CNF on this client with ID "
      << m_rootCNF->getOriginId();
    return nullptr;
  }

  return m_rootCNF.get();
}
}// namespace paracooba

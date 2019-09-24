#include "../include/paracuber/client.hpp"
#include "../include/paracuber/cadical_task.hpp"
#include "../include/paracuber/cnf.hpp"
#include "../include/paracuber/communicator.hpp"
#include "../include/paracuber/config.hpp"
#include "../include/paracuber/runner.hpp"

namespace paracuber {
Client::Client(ConfigPtr config, LogPtr log, CommunicatorPtr communicator)
  : m_config(config)
  , m_logger(log->createLogger())
  , m_communicator(communicator)
{
  m_config->m_client = this;
  m_rootCNF = std::make_shared<CNF>(
    config, log, config->getInt64(Config::Id), getDIMACSSourcePathFromConfig());
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
    PARACUBER_LOG(m_logger, LocalWarning) << errorMsg;
    return errorMsg;
  }
  return sourcePath;
}

// Approach to safely cast unique_ptr from
// https://stackoverflow.com/a/36120483
template<typename TO, typename FROM>
std::unique_ptr<TO>
static_unique_pointer_cast(std::unique_ptr<FROM>&& old)
{
  return std::unique_ptr<TO>{ static_cast<TO*>(old.release()) };
}

void
Client::solve()
{
  CaDiCaLTask::Mode mode = CaDiCaLTask::Parse;

  auto task = std::make_unique<CaDiCaLTask>(&m_cnfVarCount, mode);
  auto& finishedSignal = task->getFinishedSignal();
  task->readDIMACSFile(getDIMACSSourcePathFromConfig());
  m_communicator->getRunner()->push(std::move(task));
  finishedSignal.connect([this](const TaskResult& result) {
    m_status = result.getStatus();
    if(m_status != TaskResult::Parsed) {
      PARACUBER_LOG(m_logger, Fatal)
        << "Could not parse DIMACS source file! Status: " << result.getStatus()
        << ". Exiting Client.";
      m_communicator->exit();
      return;
    }

    // If local solving is enabled, create a new task to solve the parsed
    // formula.
    if(m_config->isClientCaDiCaLEnabled()) {
      auto task = std::make_unique<CaDiCaLTask>(
        static_cast<CaDiCaLTask&>(result.getTask()));
      task->setMode(CaDiCaLTask::Solve);
      auto& finishedSignal = task->getFinishedSignal();
      task->readDIMACSFile(getDIMACSSourcePathFromConfig());
      m_communicator->getRunner()->push(std::move(task));
      finishedSignal.connect([this](const TaskResult& result) {
        // Finished solving using client CaDiCaL!
        m_status = result.getStatus();
        m_communicator->exit();
      });
    }

    // Formula has been parsed successfully and local solver has been started.
    // The internal CNF can therefore now be fully initialised with this root
    // CNF solver task. The root CNF now has a valid unique_ptr to a completely
    // parsed CaDiCaL task.
    // This is (in theory) unsafe, but should only be required in this case. It
    // should not be required to const cast the result anywhere else.
    auto& resultMut = const_cast<TaskResult&>(result);
    m_rootCNF->setRootTask(static_unique_pointer_cast<CaDiCaLTask>(
      std::move(resultMut.getTaskPtr())));

    // After the root formula has been set, which completely builds up the CNF
    // object including the cuber::Registry object, decisions can be made.
    // Decisions must be propagated to daemons in order to solve them in
    // parallel. Decisions are always guided by the CNFTree class, which
    // knows how many decisions are left in any given path.
  });
}
}

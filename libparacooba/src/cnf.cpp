#include "../include/paracooba/cnf.hpp"
#include "../include/paracooba/assignment_serializer.hpp"
#include "../include/paracooba/cadical_task.hpp"
#include "../include/paracooba/communicator.hpp"
#include "../include/paracooba/config.hpp"
#include "../include/paracooba/cuber/registry.hpp"
#include "../include/paracooba/decision_task.hpp"
#include "../include/paracooba/networked_node.hpp"
#include "../include/paracooba/runner.hpp"
#include "../include/paracooba/task_factory.hpp"
#include "paracooba/messages/job_initiator.hpp"
#include "paracooba/messages/job_path.hpp"
#include "paracooba/messages/job_result.hpp"
#include "paracooba/messages/jobdescription.hpp"

#include <boost/asio.hpp>
#include <boost/filesystem.hpp>
#include <boost/filesystem/fstream.hpp>
#include <boost/filesystem/operations.hpp>
#include <boost/log/expressions/formatters/c_decorator.hpp>
#include <cadical/cadical.hpp>
#include <iostream>
#include <shared_mutex>
#include <vector>

extern "C"
{
#include <fcntl.h>
#include <sys/sendfile.h>
#include <sys/stat.h>
#include <sys/types.h>
}

namespace paracooba {
CNF::CNF(ConfigPtr config,
         LogPtr log,
         int64_t originId,
         std::string_view dimacsFile)
  : m_config(config)
  , m_originId(originId)
  , m_dimacsFile(dimacsFile)
  , m_log(log)
  , m_logger(log->createLogger("CNF"))
  , m_cnfTree(std::make_unique<CNFTree>(log, *this, config, originId))
{
  if(dimacsFile != "") {
    struct stat statbuf;
    int result = stat(m_dimacsFile.c_str(), &statbuf);
    if(result == -1) {
      PARACOOBA_LOG(m_logger, Fatal)
        << "Could not find file \"" << dimacsFile << "\"!";
      return;
    }

    m_fileSize = statbuf.st_size;
    m_fd = open(m_dimacsFile.c_str(), O_RDONLY);
    assert(m_fd > 0);
  }

  connectToCNFTreeSignal();
  m_jobDescriptionTransmitter = config->getCommunicator();
}
CNF::CNF(const CNF& o)
  : m_config(o.m_config)
  , m_originId(o.m_originId)
  , m_dimacsFile(o.m_dimacsFile)
  , m_log(o.m_log)
  , m_logger(o.m_log->createLogger("CNF"))
  , m_cnfTree(std::make_unique<CNFTree>(m_log, *this, o.m_config, o.m_originId))
  , m_jobDescriptionTransmitter(o.m_jobDescriptionTransmitter)
{
  connectToCNFTreeSignal();
}

CNF::~CNF()
{
  PARACOOBA_LOG(m_logger, Trace) << "Destruct CNF from " << m_dimacsFile;
}

void
CNF::send(boost::asio::ip::tcp::socket* socket, SendFinishedCB cb, bool first)
{
  SendDataStruct* data = &m_sendData[socket];

  if(first) {
    data->cb = cb;
    data->offset = 0;

    // Send the filename, so that the used compression algorithm can be known.
    // Also transfer the terminating \0.
    boost::filesystem::path p(m_dimacsFile);
    std::string dimacsBasename = p.filename().string();
    socket->write_some(
      boost::asio::buffer(dimacsBasename.c_str(), dimacsBasename.size() + 1));

    first = false;
  }

  // This is the root CNF, send over the file directly using the sendfile()
  // syscall.
  int ret = sendfile(socket->native_handle(), m_fd, &data->offset, m_fileSize);
  if(ret == -1) {
    std::cerr << "ERROR DURING SENDFILE: " << strerror(errno) << std::endl;
    m_sendData.erase(socket);

    return;
  }

  socket->async_write_some(boost::asio::null_buffers(),
                           std::bind(&CNF::sendCB, this, data, socket));
}

uint64_t
CNF::getSizeToBeSent()
{
  uint64_t size = 0;
  boost::filesystem::path p(m_dimacsFile);
  std::string dimacsBasename = p.filename().string();
  size += dimacsBasename.size() + 1;
  size += boost::filesystem::file_size(p);
  return size;
}

void
CNF::sendAllowanceMap(int64_t id, SendFinishedCB finishedCallback)
{
  // First, wait for the allowance map to be ready.
  rootTaskReady.callWhenReady([this, id, finishedCallback](CaDiCaLTask& ptr) {
    // Registry is only initialised after the root task arrived.
    getCuberRegistry().allowanceMapWaiter.callWhenReady(
      [this, id, finishedCallback](cuber::Registry::AllowanceMap& map) {
        // This indirection is required to make this work from worker threads.
        // The allowance map may take a while to generate.
        messages::JobDescription jd(m_originId);

        switch(m_cuberRegistry->getCuberMode()) {
          case cuber::Registry::LiteralFrequency: {
            assert(map.size() > 0);
            auto initiator = messages::JobInitiator();
            initiator.initAllowanceMap() = map;
            jd.insert(initiator);
            break;
          }
          case cuber::Registry::PregeneratedCubes: {
            auto ji = m_cuberRegistry->getJobInitiator();
            assert(ji);
            jd.insert(*ji);
            break;
          }
        }

        m_jobDescriptionTransmitter->transmitJobDescription(
          std::move(jd), id, [this, id, finishedCallback](bool success) {
            if(success) {
              finishedCallback();
            } else {
              PARACOOBA_LOG(m_logger, GlobalError)
                << "Could not send JobInitiator to " << id << "! VERY BAD!";
            }
          });
      });
  });
}

void
CNF::sendPath(int64_t id, const TaskSkeleton& skel, SendFinishedCB finishedCB)
{
  CNFTree::Path p = skel.p;
  assert(CNFTree::getDepth(p) > 0);
  assert(rootTaskReady.isReady());

  m_cnfTree->offloadNodeToRemote(p, id);

  messages::JobPath jp(p, skel.optionalCube);
  messages::JobDescription jd(m_originId);
  jd.insert(jp);
  m_jobDescriptionTransmitter->transmitJobDescription(
    std::move(jd), id, [this, p, id, finishedCB](bool success) {
      if(success) {
        finishedCB();
      } else {
        PARACOOBA_LOG(m_logger, GlobalError)
          << "Could not send path " << CNFTree::pathToStrNoAlloc(p) << " to "
          << id << "! Re-Add to local factory.";
        // Reset the task, so it is processed again!
        m_taskFactory->removeExternallyProcessedTask(p, id, true);
      }
    });
}

void
CNF::sendResult(int64_t id, CNFTree::Path p, SendFinishedCB finishedCallback)
{
  assert(rootTaskReady.isReady());

  std::shared_lock lock(m_resultsMutex);

  auto resultIt = m_results.find(CNFTree::cleanupPath(p));
  if(resultIt == m_results.end()) {
    PARACOOBA_LOG(m_logger, LocalError)
      << "Could not find result for path " << CNFTree::pathToStrNoAlloc(p)
      << "! Cannot send result.";
    return;
  }
  auto& result = resultIt->second;

  assert(result.state != CNFTree::Unknown);

  messages::JobResult::State jobResultState = [&result]() {
    switch(result.state) {
      case CNFTree::SAT:
        return messages::JobResult::State::SAT;
      case CNFTree::UNSAT:
        return messages::JobResult::State::UNSAT;
      default:
        return messages::JobResult::State::UNKNOWN;
    }
  }();

  assert(jobResultState != messages::JobResult::State::UNKNOWN);

  auto jobResult = messages::JobResult(result.p, jobResultState);

  if(jobResultState == messages::JobResult::State::SAT) {
    result.encodeAssignment();
    jobResult.initDataVec() = *result.encodedAssignment;
  }

  auto jd = messages::JobDescription(m_originId);
  jd.insert(jobResult);

  m_jobDescriptionTransmitter->transmitJobDescription(
    std::move(jd), id, [this, finishedCallback, p, id](bool success) {
      if(success) {
        // A result has been sent
        if(m_numberOfUnansweredRemoteWork > 0) {
          --m_numberOfUnansweredRemoteWork;
        } else {
          std::unique_lock loggerLock(m_loggerMutex);
          PARACOOBA_LOG(m_logger, LocalWarning)
            << "The number of unanswered remote work is already 0 and should "
               "be "
               "decreased again. This should not happen!";
        }
        finishedCallback();
      } else {
        PARACOOBA_LOG(m_logger, GlobalError)
          << "Could not send result for path " << CNFTree::pathToStrNoAlloc(p)
          << " back to " << id << "!";
      }
    });
}

static CNFTree::State
jrStateToCNFTreeState(messages::JobResult::State s)
{
  switch(s) {
    case messages::JobResult::SAT:
      return CNFTree::SAT;
    case messages::JobResult::UNSAT:
      return CNFTree::UNSAT;
    case messages::JobResult::UNKNOWN:
      return CNFTree::Unknown;
    default:
      return CNFTree::Unknown;
  }
}

void
CNF::receiveJobDescription(int64_t sentFromID,
                           messages::JobDescription&& jd,
                           NetworkedNode& nn)
{
  {
    std::unique_lock loggerLock(m_loggerMutex);
    PARACOOBA_LOG(m_logger, Trace)
      << "Received " << jd.tagline() << " from " << sentFromID;
  }
  switch(jd.getKind()) {
    case messages::JobDescription::Kind::Path: {
      const auto jp = jd.getJobPath();
      auto p = CNFTree::cleanupPath(jp.getPath());
      m_cnfTree->insertNodeFromRemote(p, sentFromID);

      // As a new path has been received, the number of unanswered remote work
      // increases. It is decreased again once a result is sent. This is
      // especially important for auto shutdown.
      ++m_numberOfUnansweredRemoteWork;

      // Immediately realise task, so the chain of distributing tasks cannot be
      // broken by offloading the task directly after it was inserted into the
      // factory.
      std::unique_ptr<DecisionTask> task = std::make_unique<DecisionTask>(
        shared_from_this(), p, jp.getOptionalCube());
      m_config->getCommunicator()->getRunner()->push(
        std::move(task),
        sentFromID,
        TaskFactory::getTaskPriority(TaskFactory::CubeOrSolve, p),
        m_taskFactory);
      break;
    }
    case messages::JobDescription::Kind::Result: {
      const auto jr = jd.getJobResult();

      Result* res = nullptr;

      {
        std::unique_lock lock(m_resultsMutex);

        auto [resIt, inserted] = m_results.insert(
          std::make_pair(CNFTree::cleanupPath(jr.getPath()), Result{}));

        res = &resIt->second;

        if(!inserted) {
          std::unique_lock loggerLock(m_loggerMutex);
          PARACOOBA_LOG(m_logger, GlobalWarning)
            << "Result for path " << CNFTree::pathToStrNoAlloc(jr.getPath())
            << " received from " << sentFromID
            << " already inserted into results previously! Previous result "
               "state: "
            << res->state
            << ", new state: " << jrStateToCNFTreeState(jr.getState());
        }

        res->p = jr.getPath();
        res->state = jrStateToCNFTreeState(jr.getState());
        assert(res->state != CNFTree::Unknown);

        m_taskFactory->removeExternallyProcessedTask(res->p, sentFromID);

        if(res->state == CNFTree::SAT) {
          res->size = jr.getDataVec().size();
          res->encodedAssignment =
            std::make_shared<AssignmentVector>(std::move(jr.getDataVec()));
        } else {
          res->size = 0;
        }
        res->finished = true;
      }

      handleFinishedResultReceived(*res, sentFromID);
      break;
    }
    case messages::JobDescription::Kind::Initiator: {
      rootTaskReady.callWhenReady(
        [this, jd{ std::move(jd) }](CaDiCaLTask& ptr) {
          const auto ji = jd.getJobInitiator();

          if(!m_cuberRegistry)
            m_cuberRegistry =
              std::make_unique<cuber::Registry>(m_config, m_log, *this);

          switch(ji.getCubingKind()) {
            case messages::JobInitiator::PregeneratedCubes:
              m_cuberRegistry->init(cuber::Registry::PregeneratedCubes, &ji);
              break;
            case messages::JobInitiator::LiteralFrequency:
              m_cuberRegistry->init(cuber::Registry::LiteralFrequency, &ji);
              m_cuberRegistry->getAllowanceMap() = ji.getAllowanceMap();
              break;
          }
        });
      break;
    }
    case messages::JobDescription::Kind::Unknown:
      PARACOOBA_LOG(m_logger, GlobalWarning)
        << "Received invalid job description!";
      break;
  }
}

void
CNF::connectToCNFTreeSignal()
{
  m_cnfTree->getRootStateChangedSignal().connect(
    [this](CNFTree::Path p, CNFTree::State state) {
      if(state != CNFTree::SAT && state != CNFTree::UNSAT)
        return;

      PARACOOBA_LOG(m_logger, Info) << "CNF: Found a result and send to all "
                                       "subscribers of signals! End Result: "
                                    << state;

      std::shared_lock lock(m_resultsMutex);
      Result* result = &m_results[0];
      assert(result);

      if(result->state == CNFTree::SAT) {
        // The result must contain the assignment if it is satisfiable.
        result->decodeAssignment();
      }

      m_resultSignal(result);
    });
}

void
CNF::sendCB(SendDataStruct* data, boost::asio::ip::tcp::socket* socket)
{
  if(data->offset >= m_fileSize) {
    data->cb();
    // This not only erases the send data structure,
    // but also frees the last reference to the
    // shared_ptr of the TCPClient instance that was
    // calling this function. The connection is then
    // closed.
    m_sendData.erase(socket);
  } else {
    // Repeated sends, as long as the file is transferred.
    send(socket, data->cb, false);
  }
}
bool
CNF::readyToBeStarted() const
{
  return m_rootTask && m_cuberRegistry &&
         m_cuberRegistry->allowanceMapWaiter.isReady();
}

void
CNF::solverFinishedSlot(const TaskResult& result, CNFTree::Path p)
{
  Result res;
  res.p = p;

  {
    auto& resultMut = const_cast<TaskResult&>(result);
    auto task = static_unique_pointer_cast<CaDiCaLTask>(
      std::move(resultMut.getTaskPtr()));
    res.task = std::move(task);
  }

  if(!res.task)
    return;

  res.size = res.task->getVarCount();

  switch(result.getStatus()) {
    case TaskResult::Satisfiable: {
      res.state = CNFTree::SAT;

      // Satisfiable results must be directly wrapped in the result array, as
      // the solver instances are shared between solver tasks.
      std::unique_lock loggerLock(m_loggerMutex);
      PARACOOBA_LOG(m_logger, Trace)
        << "Encode Assignment because SAT encountered!";
      res.encodeAssignment();
      break;
    }
    case TaskResult::Unsatisfiable:
      res.state = CNFTree::UNSAT;
      break;
    case TaskResult::Unsolved:
      res.state = CNFTree::Unknown;
      break;
    default: {
      // Other results are not handled here.
      std::unique_lock loggerLock(m_loggerMutex);
      PARACOOBA_LOG(m_logger, LocalError)
        << "Invalid status received for finished solver task: "
        << result.getStatus();
    }
  }

  // Give back the solver handle from the result after it was fully processed.
  res.task->releaseSolver();

  if(res.state == CNFTree::SAT || res.state == CNFTree::UNSAT ||
     res.state == CNFTree::Unknown) {
    if(res.state != CNFTree::Unknown) {
      {
        std::unique_lock lock(m_resultsMutex);
        m_results.insert(
          std::make_pair(CNFTree::cleanupPath(p), std::move(res)));
      }
      m_cnfTree->setStateFromLocal(p, res.state);
    }
  }
}

template<typename T>
T*
receiveValueFromBuffer(CNF::ReceiveDataStruct& d, char** buf, std::size_t* len)
{
  T* returnVal = nullptr;
  if(d.receiveVarPos == 0 && *len >= sizeof(T)) {
    // Easy receive.
    returnVal = reinterpret_cast<T*>(*buf);
    *buf += sizeof(T);
    *len -= sizeof(T);
  } else {
    // Cube var split into multiple buffers, reconstructing.
    size_t l = (*len >= sizeof(T) ? sizeof(T) : *len) - d.receiveVarPos;

    std::copy(*buf, *buf + l, d.receiveVarBuf + d.receiveVarPos);

    *buf += l;
    *len -= l;

    if(d.receiveVarPos == sizeof(T)) {
      d.receiveVarPos = 0;
      returnVal = reinterpret_cast<T*>(d.receiveVarBuf);
    }
  }
  return returnVal;
}

void
CNF::receive(boost::asio::ip::tcp::socket* socket,
             const char* buf,
             std::size_t length)
{
  using namespace boost::filesystem;
  assert(socket);

  ReceiveDataStruct& d = m_receiveData[socket];

  while(length > 0 || buf == nullptr) {
    switch(d.state) {
      case ReceiveFileName: {
        // First, receive the filename.
        bool end = false;
        while(length > 0 && !end) {
          if(*buf == '\0') {
            d.state = ReceiveFile;

            path dir =
              temp_directory_path() / ("paracooba-" + std::to_string(getpid()));
            path p = dir / unique_path();
            p += "-" + m_dimacsFile;

            m_dimacsFile = p.string();

            if(!exists(dir)) {
              create_directory(dir);
            }

            m_ofstream.open(m_dimacsFile, std::ios::out);
            end = true;
          } else {
            m_dimacsFile += *buf;
          }
          ++buf;
          --length;
        }
        break;
      }
      case ReceiveFile:
        if(length == 0) {
          // This marks the end of the transmission, the file is finished.
          m_ofstream.close();
          m_receiveData.erase(socket);
          return;
        }

        m_ofstream.write(buf, length);
        length = 0;
        break;
    }
  }
}

void
CNF::setRootTask(std::unique_ptr<CaDiCaLTask> root)
{
  /// This shall only be called once, the internal root task must therefore
  /// always be false. This is because this function also initialises the cubing
  /// registry in m_cuberRegistry.
  assert(!m_rootTask);
  m_rootTask = std::move(root);

  if(!m_config->isDaemonMode()) {
    assert(!m_cuberRegistry);

    const auto& pregenCubes = m_rootTask->getPregeneratedCubes();
    messages::JobInitiator ji;
    if(pregenCubes.size() > 0) {
      ji.initAsPregenCubes();
    }

    // The cuber registry may already have been created if this is a daemon
    // node. It can then just be re-used, as the daemon cuber registry did not
    // need the root node to be created.
    m_cuberRegistry = std::make_unique<cuber::Registry>(m_config, m_log, *this);
    if(!m_cuberRegistry->init(pregenCubes.size() > 0
                                ? cuber::Registry::PregeneratedCubes
                                : cuber::Registry::LiteralFrequency,
                              &ji)) {
      PARACOOBA_LOG(m_logger, Fatal) << "Could not initialise cuber registry!";
      m_cuberRegistry.reset();
      return;
    }
  }

  rootTaskReady.setReady(m_rootTask.get());
}
CaDiCaLTask*
CNF::getRootTask()
{
  return m_rootTask.get();
}

void
CNF::handleFinishedResultReceived(const Result& result, int64_t sentFromId)
{
  {
    std::unique_lock loggerLock(m_loggerMutex);
    PARACOOBA_LOG(m_logger, Trace)
      << "Finished result received! State: " << result.state << " on path "
      << CNFTree::pathToStrNoAlloc(result.p) << " from id " << sentFromId;
  }

  // Insert result into local CNF Tree. The state change should only stay local,
  // no propagate to the node that sent this change.
  m_cnfTree->setStateFromRemote(result.p, result.state, sentFromId);
}

void
CNF::insertResult(CNFTree::Path p, CNFTree::State state, CNFTree::Path source)
{
  p = CNFTree::cleanupPath(p);

  std::unique_lock unique_lock(m_resultsMutex);

  Result* res = [this, p, source, state]() {
    auto resIt = m_results.find(p);
    if(resIt != m_results.end()) {
      return &resIt->second;
    } else {
      std::unique_lock loggerLock(m_loggerMutex);
      PARACOOBA_LOG(m_logger, Trace)
        << "Insert result " << state << " for path "
        << CNFTree::pathToStdString(p) << " from source path "
        << CNFTree::pathToStdString(source);
      return &m_results.insert(std::make_pair(p, Result{ 0 })).first->second;
    }
  }();

  res->p = p;
  res->state = state;

  // Assign the path directly to 0 to make referencing it easier, if this is the
  // root path.
  if(CNFTree::getDepth(p) == 0)
    p = 0;

  if(source != CNFTree::DefaultUninitiatedPath) {
    source = CNFTree::cleanupPath(source);
    // Reference old result.
    auto resultIt = m_results.find(source);
    if(resultIt == m_results.end()) {
      PARACOOBA_LOG(m_logger, LocalError)
        << "Could not find result reference of " << CNFTree::pathToStdString(p)
        << " to " << CNFTree::pathToStdString(source)
        << "! Not inserting result.";
      assert(false);
      return;
    }
    const Result& oldResult = resultIt->second;
    res->task = oldResult.task;
    res->size = oldResult.size;
    res->encodedAssignment = oldResult.encodedAssignment;
    res->decodedAssignment = oldResult.decodedAssignment;
  }
}

void
CNF::Result::encodeAssignment()
{
  if(encodedAssignment)
    return;
  assert(decodedAssignment || task);
  encodedAssignment = std::make_shared<AssignmentVector>();

  if(decodedAssignment) {
    // Encode from currently decoded assignment.
    SerializeAssignmentFromArray(*encodedAssignment, size, *decodedAssignment);
  } else {
    // Write directly from solver.
    task->writeEncodedAssignment(*encodedAssignment);
  }
}

void
CNF::Result::decodeAssignment()
{
  if(decodedAssignment)
    return;
  assert(encodedAssignment || task);
  decodedAssignment = std::make_shared<AssignmentVector>();

  if(encodedAssignment) {
    // Use the already encoded assignment to generate decoded one.
    DeSerializeToAssignment(*decodedAssignment, *encodedAssignment, size);
  } else {
    // Receive assignment directly from solver.
    task->writeDecodedAssignment(*decodedAssignment);
  }
}

std::ostream&
operator<<(std::ostream& o, CNF::ReceiveState s)
{
  switch(s) {
    case CNF::ReceiveState::ReceiveFile:
      o << "Receive File";
      break;
    case CNF::ReceiveState::ReceiveFileName:
      o << "Receive File Name";
      break;
    default:
      o << "(! UNKNOWN RECEIVE STATE !)";
      break;
  }
  return o;
}

std::ostream&
operator<<(std::ostream& o, CNF::CubingKind k)
{
  switch(k) {
    case CNF::CubingKind::LiteralFrequency:
      o << "LiteralFrequency";
      break;
    case CNF::CubingKind::PregeneratedCubes:
      o << "PregeneratedCubes";
      break;
    default:
      o << "(! UNKNOWN CUBING KIND !)";
      break;
  }
  return o;
}
}

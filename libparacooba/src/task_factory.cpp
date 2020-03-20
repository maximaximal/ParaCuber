#include "../include/paracooba/task_factory.hpp"
#include "../include/paracooba/cadical_mgr.hpp"
#include "../include/paracooba/cadical_task.hpp"
#include "../include/paracooba/cnf.hpp"
#include "../include/paracooba/communicator.hpp"
#include "../include/paracooba/config.hpp"
#include "../include/paracooba/cuber/registry.hpp"
#include "../include/paracooba/decision_task.hpp"
#include "../include/paracooba/runner.hpp"
#include "../include/paracooba/task.hpp"

namespace paracooba {
TaskFactory::TaskFactory(ConfigPtr config,
                         LogPtr log,
                         std::shared_ptr<CNF> rootCNF)
  : m_config(config)
  , m_logger(log->createLoggerMT("TaskFactory"))
  , m_rootCNF(rootCNF)
  , m_cadicalMgr(std::make_unique<CaDiCaLMgr>())
{
  if(m_config->hasCommunicator()) {
    m_config->getCommunicator()->getRunner()->registerTaskFactory(this);
  }
}
TaskFactory::~TaskFactory()
{
  if(m_config->hasCommunicator()) {
    m_config->getCommunicator()->getRunner()->deregisterTaskFactory(this);
  }
}

void
TaskFactory::addPath(CNFTree::Path p,
                     Mode mode,
                     int64_t originator,
                     OptionalCube optionalCube)
{
  auto ptr = std::make_unique<TaskSkeleton>(mode, originator, p, optionalCube);
  /*
    For debugging task insertion. Can throw segfaults because of library
inconsistencies.

  PARACOOBA_LOG(m_logger, Trace)
    << "Pushing path " << CNFTree::pathToStrNoAlloc(p) << " with priority "
    << ptr->getPriority();
  */
  m_skeletons.push(std::move(ptr));
}

template<class Functor>
TaskFactory::ProducedTask
parametricProduceTask(PriorityQueueLockSemanticsUniquePtr<TaskSkeleton>& queue,
                      TaskFactory& factory,
                      Functor f)
{
  std::unique_lock lock(queue.getMutex());
  if(queue.empty()) {
    return { nullptr, 0, 0 };
  }
  std::unique_ptr<TaskSkeleton> skel = f(queue);
  lock.unlock();// Early unlock: This could happen in parallel.

  switch(skel->mode) {
    case TaskFactory::CubeOrSolve:
      return factory.produceCubeOrSolveTask(std::move(skel));
      break;
    case TaskFactory::Solve:
      return factory.produceSolveTask(std::move(skel));
      break;
    default:
      break;
  }

  return { nullptr, skel->originator, 0 };
}

std::unique_ptr<TaskSkeleton>
popFromFrontOfQueue(PriorityQueueLockSemanticsUniquePtr<TaskSkeleton>& queue)
{
  return queue.popNoLock();
}
std::unique_ptr<TaskSkeleton>
popFromBackOfQueue(PriorityQueueLockSemanticsUniquePtr<TaskSkeleton>& queue)
{
  return queue.popBackNoLock();
}

TaskFactory::ProducedTask
TaskFactory::produceTask()
{
  return parametricProduceTask(m_skeletons, *this, &popFromFrontOfQueue);
}

TaskSkeleton
TaskFactory::produceTaskSkeleton()
{
  return std::move(*m_skeletons.pop());
}

TaskSkeleton
TaskFactory::produceTaskSkeletonBackwards()
{
  return std::move(*m_skeletons.popBack());
}

TaskFactory::ProducedTask
TaskFactory::produceTaskBackwards()
{
  return parametricProduceTask(m_skeletons, *this, &popFromBackOfQueue);
}

int64_t
TaskFactory::getOriginId() const
{
  return m_rootCNF->getOriginId();
}

void
TaskFactory::setRootTask(CaDiCaLTask* rootTask)
{
  assert(m_cadicalMgr);
  m_cadicalMgr->setRootTask(rootTask);
}
void
TaskFactory::initWorkerSlots(size_t workers)
{
  assert(m_cadicalMgr);
  m_cadicalMgr->initWorkerSlots(workers);
}

TaskFactory::ProducedTask
TaskFactory::produceCubeOrSolveTask(std::unique_ptr<TaskSkeleton> skel)
{
  std::unique_ptr<DecisionTask> task =
    std::make_unique<DecisionTask>(m_rootCNF, skel->p, skel->optionalCube);
  return { std::move(task), skel->originator, skel->getPriority() };
}
TaskFactory::ProducedTask
TaskFactory::produceSolveTask(std::unique_ptr<TaskSkeleton> skel)
{
  CaDiCaLTask* rootTask = m_rootCNF->getRootTask();
  assert(rootTask);
  assert(m_cadicalMgr);
  std::unique_ptr<CaDiCaLTask> task = std::make_unique<CaDiCaLTask>(*rootTask);
  task->setMode(CaDiCaLTask::Solve);
  task->setCaDiCaLMgr(m_cadicalMgr.get());
  if(skel->optionalCube.has_value()) {
    task->applyCubeDeferred(skel->p, skel->optionalCube.value());
  } else {
    task->applyCubeFromCuberDeferred(skel->p, m_rootCNF->getCuberRegistry());
  }
  return { std::move(task), skel->originator, skel->getPriority() };
}

size_t
TaskFactory::getNumberOfOffloadedTasks() const
{
  std::shared_lock lock(m_externalTasksSetMapMutex);
  size_t size = 0;
  for(const auto& it : m_externalTasksSetMap) {
    const ExternalTasksSet& exSet = it.second;
    size += exSet.tasks.size();
  }
  return size;
}

size_t
TaskFactory::getNumberOfUnansweredRemoteWork() const
{
  return m_rootCNF->getNumberOfUnansweredRemoteWork();
}

void
TaskFactory::addExternallyProcessingTask(int64_t originator,
                                         CNFTree::Path p,
                                         ClusterNode& node)
{
  std::unique_lock lock(m_externalTasksSetMapMutex);

  auto it = m_externalTasksSetMap.find(node.getId());
  if(it == m_externalTasksSetMap.end()) {
    ExternalTasksSet exSet{ node };
    auto [insertedIt, inserted] = m_externalTasksSetMap.insert(
      std::make_pair(node.getId(), std::move(exSet)));
    assert(inserted);
    it = insertedIt;

    ExternalTasksSet& set = it->second;

    set.nodeOfflineSignalConnection = node.getNodeOfflineSignal().connect(
      std::bind(&TaskFactory::readdExternalTasks, this, set.node.getId()));
  }
  ExternalTasksSet& set = it->second;
  set.addTask(TaskSkeleton{ CubeOrSolve, originator, p });
}

void
TaskFactory::removeExternallyProcessedTask(CNFTree::Path p,
                                           int64_t id,
                                           bool reset)
{
  std::unique_lock lock(m_externalTasksSetMapMutex);

  auto it = m_externalTasksSetMap.find(id);
  if(it != m_externalTasksSetMap.end()) {
    ExternalTasksSet& set = it->second;
    auto skel = set.removeTask(p);
    if(skel.p != CNFTree::DefaultUninitiatedPath && reset) {
      this->getRootCNF()->getCNFTree().resetNode(skel.p);
      this->addPath(skel.p, skel.mode, skel.originator);
    }
  }
}

size_t
TaskFactory::ExternalTasksSet::readdTasks(TaskFactory* factory)
{
  assert(factory);
  for(TaskSkeleton skel : tasks) {
    factory->getRootCNF()->getCNFTree().resetNode(skel.p);
    factory->addPath(skel.p, skel.mode, skel.originator);
  }
  size_t count = tasks.size();
  tasks.clear();
  return count;
}

TaskSkeleton
TaskFactory::ExternalTasksSet::removeTask(CNFTree::Path p)
{
  for(auto it = tasks.begin(); it != tasks.end();) {
    if(it->p == p) {
      TaskSkeleton s = std::move(*it);
      it = tasks.erase(it);
      return std::move(s);
    } else
      ++it;
  }

  return std::move(
    TaskSkeleton{ CubeOrSolve, 0, CNFTree::DefaultUninitiatedPath });
}

void
TaskFactory::readdExternalTasks(int64_t id)
{
  std::unique_lock lock(m_externalTasksSetMapMutex);

  auto it = m_externalTasksSetMap.find(id);
  if(it != m_externalTasksSetMap.end()) {
    ExternalTasksSet& set = it->second;
    size_t count = set.readdTasks(this);
    m_externalTasksSetMap.erase(it);

    PARACOOBA_LOG(m_logger, Trace)
      << "Re-Added " << count
      << " tasks to local factory for task with originator "
      << m_rootCNF->getOriginId() << " which were previously sent to node "
      << id;
  }
}
}

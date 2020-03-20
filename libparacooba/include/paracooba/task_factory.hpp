#ifndef PARACOOBA_TASKFACTORY_HPP
#define PARACOOBA_TASKFACTORY_HPP

#include <boost/signals2/connection.hpp>
#include <queue>

#include "cnftree.hpp"
#include "log.hpp"
#include "paracooba/priority_queue_lock_semantics.hpp"
#include "types.hpp"
#include <atomic>
#include <shared_mutex>

namespace paracooba {
class Task;
class CaDiCaLMgr;
class CaDiCaLTask;
class TaskSkeleton;
class ClusterNode;

/** @brief Creates new tasks for the runners during the solving process.
 *
 * Every newly produced path is given to this factory. It then decides if
 * another cube shall be generated or if the path should directly be given to a
 * CDCL solver. It is only doing local actions, so selection of the correct
 * compute node must happen externally.
 */
class TaskFactory
{
  public:
  explicit TaskFactory(ConfigPtr config,
                       LogPtr log,
                       std::shared_ptr<CNF> rootCNF);
  ~TaskFactory();

  enum Mode
  {
    CubeOrSolve,
    Solve
  };

  static inline int getTaskPriority(Mode mode, CNFTree::Path p)
  {
    return (CNFTree::maxPathDepth - CNFTree::getDepth(p)) +
           (mode != Solve ? 100 : 0);
  }

  struct ProducedTask
  {
    std::unique_ptr<Task> task;
    int64_t originator;
    int priority;
  };

  void addPath(CNFTree::Path p,
               Mode m,
               int64_t originator,
               OptionalCube optionalCube = std::nullopt);

  ProducedTask produceTask();
  ProducedTask produceTaskBackwards();

  TaskSkeleton produceTaskSkeleton();
  TaskSkeleton produceTaskSkeletonBackwards();

  void addExternallyProcessingTask(int64_t originator,
                                   CNFTree::Path p,
                                   ClusterNode& node);

  void removeExternallyProcessedTask(CNFTree::Path p,
                                     int64_t id,
                                     bool reset = false);
  void readdExternalTasks(int64_t id);

  inline bool canProduceTask() const { return !m_skeletons.empty(); }
  inline uint64_t getSize() const { return m_skeletons.size(); }
  int64_t getOriginId() const;
  std::shared_ptr<CNF> getRootCNF() const { return m_rootCNF; }

  void setRootTask(CaDiCaLTask* rootTask);
  void initWorkerSlots(size_t workers);

  ProducedTask produceCubeOrSolveTask(std::unique_ptr<TaskSkeleton> skel);
  ProducedTask produceSolveTask(std::unique_ptr<TaskSkeleton> skel);

  size_t getNumberOfOffloadedTasks() const;
  size_t getNumberOfUnansweredRemoteWork() const;

  CaDiCaLMgr* getCaDiCaLMgr()
  {
    assert(m_cadicalMgr);
    return m_cadicalMgr.get();
  }

  private:
  struct ExternalTasksSet
  {
    ClusterNode& node;
    boost::signals2::scoped_connection nodeOfflineSignalConnection;
    std::set<TaskSkeleton> tasks;

    size_t readdTasks(TaskFactory* factory);
    void addTask(TaskSkeleton&& skel) { tasks.insert(std::move(skel)); }
    TaskSkeleton removeTask(CNFTree::Path p);
  };
  using ExternalTasksSetMap = std::map<int64_t, ExternalTasksSet>;

  ConfigPtr m_config;
  LoggerMT m_logger;
  PriorityQueueLockSemanticsUniquePtr<TaskSkeleton> m_skeletons;
  ExternalTasksSetMap m_externalTasksSetMap;
  std::shared_ptr<CNF> m_rootCNF;
  std::unique_ptr<CaDiCaLMgr> m_cadicalMgr;

  mutable std::shared_mutex m_externalTasksSetMapMutex;
};

class TaskSkeleton
{
  public:
  TaskFactory::Mode mode;
  int64_t originator;
  CNFTree::Path p;
  OptionalCube optionalCube;

  TaskSkeleton(TaskFactory::Mode mode,
               int64_t originator,
               CNFTree::Path p,
               OptionalCube optionalCube = std::nullopt)
    : mode(mode)
    , originator(originator)
    , p(p)
    , optionalCube(optionalCube)
  {}

  inline int getPriority() const
  {
    return TaskFactory::getTaskPriority(mode, p);
  }

  inline bool operator<(TaskSkeleton const& b) const
  {
    return getPriority() > b.getPriority();
  }

  inline bool operator==(const TaskSkeleton& b) const
  {
    return mode == b.mode && originator == b.originator && p == b.p;
  }
};
}

#endif

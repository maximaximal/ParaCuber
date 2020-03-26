#ifndef PARACOOBA_RUNNER_HPP
#define PARACOOBA_RUNNER_HPP

#include "log.hpp"
#include <atomic>
#include <boost/asio/high_resolution_timer.hpp>
#include <condition_variable>
#include <future>
#include <mutex>
#include <queue>
#include <shared_mutex>
#include <thread>
#include <vector>

#include "priority_queue_lock_semantics.hpp"

namespace paracooba {
class Communicator;
class Task;
class TaskResult;
class TaskFactory;

/** @brief Environment for running a \ref Task in.
 *
 * This class implements a thread pool of running worker threads, each
 * executing \ref Task objects.
 */
class Runner
{
  public:
  /** @brief Create a runner for tasks.
   *
   * This constructor does not start the internal thread pool yet. */
  Runner(Communicator* communicator, ConfigPtr config, LogPtr log);
  /** Destructor */
  ~Runner();

  /** @brief Start the thread-pool asynchronously.
   *
   * This function returns immediately. */
  void start();
  /** @brief Ends the thread-pool synchronously.
   *
   * This function returns once the last thread has finished. */
  void stop();

  /** @brief Reports if the runner is already running. */
  inline bool isRunning() const { return m_running; }

  inline uint64_t getWorkQueueSize() const { return m_taskQueue->size(); }

  /** @brief Push a new task to the internal task queue.
   *
   * The task will be run as soon as priorities, dependencies, ..., are sorted
   * out.
   *
   * @param task The task to schedule.
   * @param originator The compute node ID that pushed this task.
   * @param priority Priority of the new task. Higher is more important.
   * @param factory May contain the factory assigned to this task, if
   * applicable.
   */
  std::future<std::unique_ptr<TaskResult>> push(std::unique_ptr<Task> task,
                                                int64_t originator,
                                                int priority = 0,
                                                TaskFactory* factory = nullptr);

  void registerTaskFactory(TaskFactory* f);
  void deregisterTaskFactory(TaskFactory* f);

  using TaskFactoryVector = std::vector<TaskFactory*>;
  ConstSharedLockView<TaskFactoryVector> getTaskFactories()
  {
    return { m_taskFactories,
             std::move(std::shared_lock(m_taskFactoriesMutex)) };
  }

  private:
  friend class Communicator;

  ConfigPtr m_config;
  LogPtr m_log;
  Logger m_logger;
  Communicator* m_communicator;
  std::atomic<bool> m_running = true;
  std::atomic<bool> m_autoShutdownArmed = false;

  std::vector<std::thread> m_pool;

  void worker(uint32_t workerId);

  struct QueueEntry
  {
    /** @brief Quick Constructor for a QueueEntry object.
     */
    QueueEntry(std::unique_ptr<Task> task,
               int priority,
               int64_t originator,
               TaskFactory* factory = nullptr);
    ~QueueEntry();
    std::unique_ptr<Task> task;
    std::promise<std::unique_ptr<TaskResult>> result;
    TaskFactory* factory = nullptr;
    int priority = 0;
    int64_t originator = 0;

    inline bool operator<(QueueEntry const& b) const
    {
      return priority > b.priority;
    }
  };

  size_t getNumberOfCurrentlyOffloadedJobs() const;
  size_t getNumberOfUnansweredRemoteWork() const;

  std::unique_ptr<PriorityQueueLockSemanticsUniquePtr<QueueEntry>> m_taskQueue;

  std::condition_variable m_newTasks;

  /// Used against spurious wake-ups:
  /// https://en.cppreference.com/w/cpp/thread/condition_variable
  bool m_newTasksVerifier = false;

  std::vector<Task*> m_currentlyRunningTasks;
  std::atomic<uint32_t> m_numberOfRunningTasks;

  mutable std::shared_mutex m_taskFactoriesMutex;
  TaskFactoryVector m_taskFactories;

  boost::asio::high_resolution_timer m_autoShutdownTimer;

  void conditionallySetAutoShutdownTimer();
  void resetAutoShutdownTimer();
  std::mutex m_autoShutdownTimerMutex;

  void checkTaskFactories();
};
}

#endif

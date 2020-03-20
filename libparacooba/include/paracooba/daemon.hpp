#ifndef PARACOOBA_DAEMON_HPP
#define PARACOOBA_DAEMON_HPP

#include "cluster-statistics.hpp"
#include "log.hpp"
#include "util.hpp"
#include "ema.hpp"
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>

namespace paracooba {
class CNF;
class Communicator;
class TaskFactory;

/** @brief Daemonised solver mode that waits for tasks and sends and receives
 * statistics to/from other solvers.
 */
class Daemon
{
  public:
  class Context
  {
    public:
    explicit Context(std::shared_ptr<CNF> rootCNF,
                     int64_t originatorID,
                     Daemon* daemon,
                     ClusterStatistics::Node& statisticsNode);
    ~Context();

    enum State
    {
      JustCreated = 0b00000000u,
      FormulaReceived = 0b00000001u,
      AllowanceMapReceived = 0b00000010u,
      ResultReceived = 0b00010000u,
      FormulaParsed = 0b00000100u,
      WaitingForWork = 0b00001000u
    };

    void start(State change);
    std::shared_ptr<CNF> getRootCNF() { return m_rootCNF; }
    inline State getState() const { return m_state; }
    inline int64_t getOriginatorId() const { return m_originatorID; }
    inline bool getReadyForWork() const { return m_readyForWork; }
    uint64_t getFactoryQueueSize() const;
    TaskFactory* getTaskFactory() { return m_taskFactory.get(); };

    private:
    friend class Daemon;

    void nodeOffline(const std::string& reason);

    std::shared_ptr<CNF> m_rootCNF;
    std::unique_ptr<TaskFactory> m_taskFactory;
    int64_t m_originatorID = 0;
    Daemon* m_daemon;
    Logger m_logger;

    State m_state = JustCreated;
    bool m_readyForWork = false;

    std::mutex m_contextMutex;

    ClusterStatistics::Node& m_statisticsNode;

    boost::signals2::connection m_nodeOfflineSignalConnection;
  };

  /** @brief Constructor
   */
  Daemon(ConfigPtr config,
         LogPtr log,
         std::shared_ptr<Communicator> communicator);
  /** @brief Destructor
   */
  ~Daemon();

  using ContextMap = std::map<int64_t, std::unique_ptr<Context>>;

  UniqueLockView<ContextMap&> getUniqueContextMap();
  ConstSharedLockView<ContextMap> getContextMap();
  SharedLockView<Context*> getContext(int64_t id);

  std::pair<Context&, bool> getOrCreateContext(int64_t id);

  void forgetAboutContext(int64_t id);

  /// duplicates an already exsisting context and makes it
  /// a new context
  void duplicateContextWithNewId(int64_t oldId, int64_t currId, CNFTree::Path p);

  private:
  ConfigPtr m_config;
  ContextMap m_contextMap;
  std::shared_mutex m_contextMapMutex;
  LogPtr m_log;
  Logger m_logger;
  std::shared_ptr<Communicator> m_communicator;
  EMA time_ema;
};

inline Daemon::Context::State
operator|(Daemon::Context::State a, Daemon::Context::State b)
{
  return static_cast<Daemon::Context::State>(static_cast<int>(a) |
                                             static_cast<int>(b));
}
inline Daemon::Context::State operator&(Daemon::Context::State a,
                                        Daemon::Context::State b)
{
  return static_cast<Daemon::Context::State>(static_cast<int>(a) &
                                             static_cast<int>(b));
}
}

#endif

#ifndef PARACOOBA_CNF_HPP
#define PARACOOBA_CNF_HPP

#include <atomic>
#include <boost/signals2/signal.hpp>
#include <fstream>
#include <functional>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <string_view>
#include <vector>

#include <boost/asio/ip/tcp.hpp>

#include "cnftree.hpp"
#include "log.hpp"
#include "readywaiter.hpp"
#include "taskresult.hpp"

#include "messages/jobdescription_receiver.hpp"
#include "messages/jobdescription_transmitter.hpp"

namespace paracooba {
class NetworkedNode;
class CaDiCaLTask;
class TaskFactory;
class TaskSkeleton;
class ClusterNodeStore;

namespace cuber {
class Registry;
}

/** @brief This class represents a CNF formula that can be transferred directly.
 *
 * A dimacs file can be sent, if it is the root formula. All appended cubes
 * are transferred only afterwards, the root formula is not touched again.
 */
class CNF
  : public std::enable_shared_from_this<CNF>
  , public messages::JobDescriptionReceiver
{
  public:
  /** @brief Construct a CNF from existing literals based on DIMACS file or on
   * previous CNF.
   */
  CNF(ConfigPtr config,
      LogPtr log,
      int64_t originId,
      ClusterNodeStore& clusterNodeStore,
      std::string_view dimacsFile = "");

  /** @brief Copy another CNF formula.
   */
  CNF(const CNF& o);
  virtual ~CNF();

  enum ReceiveState
  {
    ReceiveFileName,
    ReceiveFile,
  };

  using AssignmentVector = std::vector<uint8_t>;
  using AssignmentVectorPtr = std::shared_ptr<AssignmentVector>;

  struct Result
  {
    Path p;
    CNFTree::State state;
    uint32_t size = 0;
    bool finished = false;
    AssignmentVectorPtr encodedAssignment;
    AssignmentVectorPtr decodedAssignment;
    std::shared_ptr<CaDiCaLTask> task;

    void encodeAssignment();
    void decodeAssignment();
  };

  struct ReceiveDataStruct
  {
    ReceiveState state = ReceiveFileName;
    Path path = 0;
    uint8_t currentDepth = 0;
    size_t receiveVarPos = 0;
    char receiveVarBuf[8];
    int64_t originator = 0;
    Result* result = nullptr;
  };

  std::string_view getDimacsFile() { return m_dimacsFile; }

  using SendFinishedCB = std::function<void()>;
  using ResultFoundSignal = boost::signals2::signal<void(Result*)>;

  /** @brief Send the underlying DIMACS file to the given socket. */
  void send(boost::asio::ip::tcp::socket* socket,
            SendFinishedCB finishedCallback,
            bool first = true);
  uint64_t getSizeToBeSent();

  void sendAllowanceMap(NetworkedNode &nn, SendFinishedCB finishedCB);

  void sendPath(int64_t id,
                const TaskSkeleton& skel,
                SendFinishedCB finishedCB);

  void sendResult(int64_t id, Path p, SendFinishedCB finishedCallback);

  /** @brief Receive DIMACS file.
   *
   * This writes directly to disk and contains the
   * filename! */
  void receive(boost::asio::ip::tcp::socket* socket,
               const char* buf,
               std::size_t length);

  virtual void receiveJobDescription(int64_t sentFromID,
                                     messages::JobDescription&& jd,
                                     NetworkedNode& nn);

  inline int64_t getOriginId() { return m_originId; }

  void setRootTask(std::unique_ptr<CaDiCaLTask> root);
  CaDiCaLTask* getRootTask();

  cuber::Registry& getCuberRegistry() { return *m_cuberRegistry; }

  ReadyWaiter<CaDiCaLTask> rootTaskReady;

  bool readyToBeStarted() const;

  CNFTree& getCNFTree() { return *m_cnfTree; }

  void requestInfoGlobally(Path p, int64_t handle = 0);

  void setTaskFactory(TaskFactory* f) { m_taskFactory = f; }
  TaskFactory* getTaskFactory() const { return m_taskFactory; }

  void solverFinishedSlot(const TaskResult& result, Path path);

  void handleFinishedResultReceived(const Result& result, int64_t sentFromId);

  ResultFoundSignal& getResultFoundSignal() { return m_resultSignal; }

  void insertResult(Path p, CNFTree::State state, Path source);

  size_t getNumberOfUnansweredRemoteWork() const
  {
    return m_numberOfUnansweredRemoteWork;
  }

  enum CubingKind
  {
    LiteralFrequency,
    PregeneratedCubes
  };

  private:
  struct SendDataStruct
  {
    off_t offset = 0;
    SendFinishedCB cb;
    Cube decisions;
  };

  ConfigPtr m_config;
  LogPtr m_log;
  Logger m_logger;
  std::mutex m_loggerMutex;
  ClusterNodeStore& m_clusterNodeStore;

  void connectToCNFTreeSignal();

  void sendCB(SendDataStruct* data, boost::asio::ip::tcp::socket* socket);

  int64_t m_originId = 0;
  std::string m_dimacsFile = "";

  CubingKind m_cubingKind = LiteralFrequency;

  // Ofstream for outputting the original CNF file.
  std::ofstream m_ofstream;

  int m_fd = 0;
  size_t m_fileSize = 0;

  std::map<boost::asio::ip::tcp::socket*, SendDataStruct> m_sendData;
  std::map<boost::asio::ip::tcp::socket*, ReceiveDataStruct> m_receiveData;

  using CaDiCaLTaskPtr = std::unique_ptr<CaDiCaLTask>;
  CaDiCaLTaskPtr m_rootTask;

  std::unique_ptr<cuber::Registry> m_cuberRegistry;
  std::unique_ptr<CNFTree> m_cnfTree;
  std::shared_mutex m_resultsMutex;

  std::map<Path, Result> m_results;

  TaskFactory* m_taskFactory = nullptr;
  ResultFoundSignal m_resultSignal;

  std::atomic_size_t m_numberOfUnansweredRemoteWork = 0;
};

std::ostream&
operator<<(std::ostream& m, CNF::ReceiveState s);
std::ostream&
operator<<(std::ostream& m, CNF::CubingKind k);
}

#endif

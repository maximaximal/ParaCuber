#ifndef PARACOOBA_NET_CONNECTION
#define PARACOOBA_NET_CONNECTION

#include <atomic>
#include <memory>
#include <mutex>
#include <queue>
#include <variant>

#include <boost/asio/coroutine.hpp>
#include <boost/asio/io_service.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/streambuf.hpp>

#include <boost/system/error_code.hpp>

#include "../daemon.hpp"
#include "../log.hpp"

#include "../messages/jobdescription_receiver.hpp"
#include "../messages/message_receiver.hpp"

namespace paracooba {
class CNF;
class ClusterNodeStore;

namespace messages {
class Message;
class JobDescription;
}

namespace net {
/** @brief Class representing a connection between two compute nodes.
 *
 * This class handles the complete connection live-cycle. Connections are
 * established over TCP sockets. If a socket is ended, it is tried to reconnect
 * to the remote host. Amount of retries can be tuned with
 * Config::ConnectionRetries.
 */
class Connection
{
  public:
  enum Mode
  {
    TransmitCNF,
    TransmitJobDescription,
    TransmitControlMessage,
    TransmitModeUnknown
  };

  enum ResumeMode
  {
    EndAfterShutdown,
    RestartAfterShutdown
  };

  using SendItem = std::
    variant<std::shared_ptr<CNF>, messages::Message, messages::JobDescription>;
  using ContextPtr = Daemon::Context*;

  struct SendQueueEntry
  {
    SendQueueEntry();
    SendQueueEntry(const SendQueueEntry& o);
    SendQueueEntry(std::shared_ptr<CNF> cnf);
    SendQueueEntry(const messages::Message& msg);
    SendQueueEntry(const messages::JobDescription& jd);
    ~SendQueueEntry();

    std::unique_ptr<SendItem> sendItem;
  };

  struct State
  {
    explicit State(boost::asio::io_service& ioService,
                   LogPtr log,
                   ConfigPtr config,
                   ClusterNodeStore& clusterNodeStore,
                   messages::MessageReceiver& msgRec,
                   messages::JobDescriptionReceiverProvider& jdRecProv);
    ~State();

    boost::asio::io_service& ioService;
    boost::asio::ip::tcp::socket socket;
    LogPtr log;
    Logger logger;
    ConfigPtr config;
    ClusterNodeStore& clusterNodeStore;
    messages::MessageReceiver& messageReceiver;
    messages::JobDescriptionReceiverProvider& jobDescriptionReceiverProvider;
    NetworkedNode* remoteNN = nullptr;
    std::shared_ptr<CNF> cnf;

    boost::asio::streambuf recvStreambuf = boost::asio::streambuf();
    boost::asio::streambuf sendStreambuf = boost::asio::streambuf();
    ContextPtr context = nullptr;
    int64_t remoteId = 0;
    int64_t thisId = 0;
    uint64_t size = 0;
    uint64_t sendSize = 0;

    Mode currentMode = TransmitModeUnknown;
    Mode sendMode = TransmitModeUnknown;
    ResumeMode resumeMode = EndAfterShutdown;

    std::mutex sendQueueMutex;
    std::queue<SendQueueEntry> sendQueue;
    std::unique_ptr<SendItem> currentSendItem;

    boost::asio::coroutine readCoro;
    boost::asio::coroutine writeCoro;

    std::atomic_bool connectionEstablished = false;
    std::atomic_bool connectionEnding = false;
    std::atomic_bool currentlySending = true;
    std::atomic_uint16_t connectionTry = 0;
  };

  explicit Connection(
    boost::asio::io_service& ioService,
    LogPtr log,
    ConfigPtr config,
    ClusterNodeStore& clusterNodeStore,
    messages::MessageReceiver& msgReceiver,
    messages::JobDescriptionReceiverProvider& jdReceiverProvider);

  virtual ~Connection();

  void sendCNF(std::shared_ptr<CNF> cnf);
  void sendMessage(const messages::Message& msg);
  void sendJobDescription(const messages::JobDescription& jd);
  void sendSendQueueEntry(SendQueueEntry&& e);

  void readHandler(boost::system::error_code ec = boost::system::error_code(),
                   size_t n = 0);

  boost::asio::ip::tcp::socket& socket() { return m_state->socket; }

  void connect(NetworkedNode& nn);

  bool isConnectionEstablished() const
  {
    return m_state->connectionEstablished;
  }
  bool isConnectionEnding() const { return m_state->connectionEnding; }
  uint16_t getConnectionTries() const { return m_state->connectionTry; }
  int64_t getRemoteId() const { return m_state->remoteId; }

  Mode getRecvMode() const { return m_state->currentMode; }
  Mode getSendMode() const { return m_state->sendMode; }

  private:
  void writeHandler(boost::system::error_code ec = boost::system::error_code(),
                    size_t n = 0);

  boost::asio::streambuf& recvStreambuf() { return m_state->recvStreambuf; }
  boost::asio::streambuf& sendStreambuf() { return m_state->sendStreambuf; }
  Logger& logger() { return m_state->logger; }
  int64_t& thisId() { return m_state->thisId; }
  int64_t& remoteId() { return m_state->remoteId; }
  uint64_t& size() { return m_state->size; }
  uint64_t& sendSize() { return m_state->sendSize; }
  uint8_t& currentModeAsUint8()
  {
    return reinterpret_cast<uint8_t&>(m_state->currentMode);
  }
  Mode& currentMode() { return m_state->currentMode; }
  uint8_t& sendModeAsUint8()
  {
    return reinterpret_cast<uint8_t&>(m_state->sendMode);
  }
  Mode& sendMode() { return m_state->sendMode; }

  ResumeMode& resumeMode() { return m_state->resumeMode; }
  std::mutex& sendQueueMutex() { return m_state->sendQueueMutex; }
  std::queue<SendQueueEntry>& sendQueue() { return m_state->sendQueue; }
  std::unique_ptr<SendItem>& currentSendItem()
  {
    return m_state->currentSendItem;
  }
  ConfigPtr& config() { return m_state->config; }
  ClusterNodeStore& clusterNodeStore() { return m_state->clusterNodeStore; }
  ContextPtr& context() { return m_state->context; }
  std::shared_ptr<CNF>& cnf() { return m_state->cnf; }
  NetworkedNode*& remoteNN() { return m_state->remoteNN; }
  messages::MessageReceiver& messageReceiver()
  {
    return m_state->messageReceiver;
  }
  messages::JobDescriptionReceiverProvider& jobDescriptionReceiverProvider()
  {
    return m_state->jobDescriptionReceiverProvider;
  }

  std::atomic_bool& connectionEstablished()
  {
    return m_state->connectionEstablished;
  }
  std::atomic_bool& connectionEnding() { return m_state->connectionEnding; }
  std::atomic_bool& currentlySending() { return m_state->currentlySending; }
  std::atomic_uint16_t& connectionTry() { return m_state->connectionTry; }

  boost::asio::coroutine& readCoro() { return m_state->readCoro; }
  boost::asio::coroutine& writeCoro() { return m_state->writeCoro; }

  static Mode getSendModeFromSendItem(const SendItem& sendItem);

  std::shared_ptr<State> m_state;

  bool getLocalCNF();
  void receiveIntoCNF(size_t bytes_received);
  void receiveSerializedMessage(size_t bytes_received);

  void popNextSendItem();
  bool initRemoteNN();

  void enrichLogger();
};

constexpr const char*
ConnectionModeToStr(Connection::Mode mode)
{
  switch(mode) {
    case Connection::Mode::TransmitCNF:
      return "TransmitCNF";
    case Connection::Mode::TransmitJobDescription:
      return "TransmitJobDescription";
    case Connection::Mode::TransmitControlMessage:
      return "TransmitControlMessage";
    case Connection::Mode::TransmitModeUnknown:
      return "TransmitModeUnknown";
  }
  return "Unknown Mode";
}

constexpr const char*
ConnectionResumeModeToStr(Connection::ResumeMode resumeMode)
{
  switch(resumeMode) {
    case Connection::ResumeMode::EndAfterShutdown:
      return "EndAfterShutdown";
    case Connection::ResumeMode::RestartAfterShutdown:
      return "RestartAfterShutdown";
  }
  return "Unknown Resume Mode";
}

std::ostream&
operator<<(std::ostream& o, Connection::Mode mode);

std::ostream&
operator<<(std::ostream& o, Connection::ResumeMode resumeMode);
}
}

#endif

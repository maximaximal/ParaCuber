#include "../../include/paracooba/net/connection.hpp"
#include "../../include/paracooba/cluster-node-store.hpp"
#include "../../include/paracooba/cluster-node.hpp"
#include "../../include/paracooba/cnf.hpp"
#include "../../include/paracooba/config.hpp"
#include "../../include/paracooba/messages/jobdescription.hpp"
#include "../../include/paracooba/messages/message.hpp"
#include "../../include/paracooba/networked_node.hpp"

#include <algorithm>
#include <boost/asio/buffer.hpp>
#include <boost/asio/placeholders.hpp>
#include <functional>
#include <mutex>

#include <boost/asio.hpp>
#include <boost/asio/coroutine.hpp>
#include <cereal/archives/binary.hpp>
#include <sstream>

namespace paracooba {
namespace net {

#define REC_BUF_SIZE static_cast<uint64_t>(4096)

static const std::string& ConnectionLoggerName = "Connection";

Connection::State::State(boost::asio::io_service& ioService,
                         LogPtr log,
                         ConfigPtr config,
                         ClusterNodeStore& clusterNodeStore,
                         messages::MessageReceiver& msgRec,
                         messages::JobDescriptionReceiverProvider& jdRecProv,
                         uint16_t retries)
  : ioService(ioService)
  , socket(ioService)
  , log(log)
  , logger(log->createLogger(ConnectionLoggerName))
  , config(config)
  , clusterNodeStore(clusterNodeStore)
  , messageReceiver(msgRec)
  , jobDescriptionReceiverProvider(jdRecProv)
  , connectionTry(retries)
{
  thisId = config->getInt64(Config::Id);
}

Connection::State::~State()
{
  PARACOOBA_LOG(logger, NetTrace)
    << "Connection Ended with resume mode " << resumeMode;
  if(remoteNN) {
    remoteNN->removeActiveTCPClient();
    remoteNN->resetConnection();
  }

  switch(resumeMode) {
    case RestartAfterShutdown: {
      // Attempt restart.
      if(remoteNN) {
        Connection conn(ioService,
                        log,
                        config,
                        clusterNodeStore,
                        messageReceiver,
                        jobDescriptionReceiverProvider,
                        connectionTry);
        conn.connect(*remoteNN);

        // Also apply all old queue entries to send.
        while(!sendQueue.empty()) {
          auto entry = sendQueue.front();
          sendQueue.pop();
          conn.sendSendQueueEntry(std::move(entry));
        }
      } else {
        PARACOOBA_LOG(logger, NetTrace)
          << "Cannot restart connection, as no remoteNN is set!";
      }
      break;
    }
    case EndAfterShutdown:
      // Entries could not be sent, so notify callbacks.
      while(!sendQueue.empty()) {
        auto entry = sendQueue.front();
        sendQueue.pop();
        entry.sendFinishedCB(false);
      }
      break;
  }
}

Connection::SendQueueEntry::SendQueueEntry() {}
Connection::SendQueueEntry::SendQueueEntry(const SendQueueEntry& o,
                                           SuccessCB sendFinishedCB)
  : sendItem(std::make_unique<SendItem>(*o.sendItem))
  , sendFinishedCB(sendFinishedCB)
{}
Connection::SendQueueEntry::SendQueueEntry(std::shared_ptr<CNF> cnf,
                                           SuccessCB sendFinishedCB)
  : sendItem(std::make_unique<SendItem>(cnf))
  , sendFinishedCB(sendFinishedCB)
{}
Connection::SendQueueEntry::SendQueueEntry(const messages::Message& msg,
                                           SuccessCB sendFinishedCB)
  : sendItem(std::make_unique<SendItem>(msg))
  , sendFinishedCB(sendFinishedCB)
{}
Connection::SendQueueEntry::SendQueueEntry(const messages::JobDescription& jd,
                                           SuccessCB sendFinishedCB)
  : sendItem(std::make_unique<SendItem>(jd))
  , sendFinishedCB(sendFinishedCB)
{}
Connection::SendQueueEntry::SendQueueEntry(EndTokenTag endTokenTag,
                                           SuccessCB sendFinishedCB)
  : sendItem(std::make_unique<SendItem>(endTokenTag))
  , sendFinishedCB(sendFinishedCB)
{}
Connection::SendQueueEntry::~SendQueueEntry() {}

Connection::Connection(boost::asio::io_service& ioService,
                       LogPtr log,
                       ConfigPtr config,
                       ClusterNodeStore& clusterNodeStore,
                       messages::MessageReceiver& msgRec,
                       messages::JobDescriptionReceiverProvider& jdRecProv,
                       uint16_t retries)
  : m_state(std::make_shared<State>(ioService,
                                    log,
                                    config,
                                    clusterNodeStore,
                                    msgRec,
                                    jdRecProv,
                                    retries))
{}
Connection::Connection(const Connection& conn)
  : m_state(conn.m_state)
{}

Connection::~Connection()
{
  if(sendMode() != TransmitEndToken && m_state.use_count() == 1) {
    // This is the last connection, so this is also the last one having a
    // reference to the state. This is a clean shutdown of a connection, the
    // socket will be ended. To differentiate between this clean shutdown and a
    // dirty one, an EndToken must be transmitted. This is the last action.
    Connection(*this).sendEndToken();
  }
}

void
Connection::sendCNF(std::shared_ptr<CNF> cnf, SuccessCB sendFinishedCB)
{
  sendSendQueueEntry(SendQueueEntry(cnf, sendFinishedCB));
}
void
Connection::sendMessage(const messages::Message& msg, SuccessCB sendFinishedCB)
{
  sendSendQueueEntry(SendQueueEntry(msg, sendFinishedCB));
}
void
Connection::sendJobDescription(const messages::JobDescription& jd,
                               SuccessCB sendFinishedCB)
{
  sendSendQueueEntry(SendQueueEntry(jd, sendFinishedCB));
}
void
Connection::sendEndToken()
{
  sendSendQueueEntry(SendQueueEntry(EndTokenTag()));
}
void
Connection::sendSendQueueEntry(SendQueueEntry&& e)
{
  {
    std::lock_guard lock(sendQueueMutex());
    sendQueue().push(e);
  }
  if(!currentlySending()) {
    writeHandler();
  }
}

#include <boost/asio/yield.hpp>

#define BUF(SOURCE) boost::asio::buffer(&SOURCE(), sizeof(SOURCE()))

void
Connection::readHandler(boost::system::error_code ec, size_t bytes_received)
{
  using namespace boost::asio;
  auto rh = std::bind(&Connection::readHandler,
                      *this,
                      std::placeholders::_1,
                      std::placeholders::_2);

  if(remoteNN() && remoteNN()->deletionRequested()) {
    return;
  }

  if(!ec) {
    reenter(&readCoro())
    {
      yield async_write(socket(), BUF(thisId), rh);

      currentlySending() = false;
      // Write pending items from queue.
      writeHandler();

      yield async_read(socket(), BUF(remoteId), rh);

      if(!initRemoteNN()) {
        // Exit coroutine and connection. This connection is already established
        // and is full duplex, the other direction is not required!
        PARACOOBA_LOG(logger(), NetTrace)
          << "Connection is already established from other side, connection "
             "will be dropped.";
        yield break;
      }
      connectionEstablished() = true;
      remoteNN()->addActiveTCPClient();

      // Handshake finished, both sides know about the other side. Communication
      // can begin now. Communication runs in a loop, as unlimited messages may
      // be received over a connection.

      for(;;) {
        yield async_read(socket(), BUF(currentModeAsUint8), rh);

        if(currentMode() == TransmitEndToken) {
          // A successfully terminated connection does not need to be restarted
          // immediately.
          resumeMode() = EndAfterShutdown;
        } else if(currentMode() == TransmitCNF) {
          if(!getLocalCNF())
            return;
          yield async_read(socket(), BUF(size), rh);
          do {
            yield async_read(
              socket(),
              recvStreambuf().prepare(std::min(size(), REC_BUF_SIZE)),
              rh);
            size() -= bytes_received;

            receiveIntoCNF(bytes_received);
          } while(size() > 0);
          context()->start(Daemon::Context::State::FormulaReceived);
        } else if(currentMode() == TransmitJobDescription ||
                  currentMode() == TransmitControlMessage) {
          yield async_read(socket(), BUF(size), rh);
          yield async_read(socket(), recvStreambuf().prepare(size()), rh);

          receiveSerializedMessage(bytes_received);
        } else {
          default:
            PARACOOBA_LOG(logger(), LocalError)
              << "Unknown message received! Connection must be restarted.";

            resumeMode() = RestartAfterShutdown;
            socket().shutdown(boost::asio::ip::tcp::socket::shutdown_both);
            socket().close();
        }
      }
    }
  } else if(ec == boost::asio::error::eof) {
    // Socket has been closed, connection ended. This can happen on purpose
    // (closed client session)

    if(resumeMode() == RestartAfterShutdown) {
      connect(*remoteNN());
    }
  }
}

void
Connection::writeHandler(boost::system::error_code ec, size_t bytes_transferred)
{
  using namespace boost::asio;
  auto wh = std::bind(&Connection::writeHandler,
                      *this,
                      std::placeholders::_1,
                      std::placeholders::_2);

  std::shared_ptr<CNF>* cnf;
  messages::JobDescription* jd;
  messages::Message* msg;
  bool repeat;

  if(!ec) {
    reenter(&writeCoro())
    {
      while(true) {
        while(!currentSendItem()) {
          yield popNextSendItem();
        }
        currentlySending() = true;

        sendMode() = getSendModeFromSendItem(*currentSendItem());
        if(sendMode() == TransmitModeUnknown)
          continue;

        yield async_write(socket(), BUF(sendModeAsUint8), wh);

        if(std::holds_alternative<EndTokenTag>(*currentSendItem())) {
          // Nothing else needs to be done, the end token has no other payload.
          // It is sent when the connection terminates normally, but it is no
          // offline announcement.
        } else if((cnf = std::get_if<std::shared_ptr<CNF>>(
                     currentSendItem().get()))) {
          sendSize() = (*cnf)->getSizeToBeSent();
          yield async_write(socket(), BUF(sendSize), wh);

          // Let sending of potentially huge CNF be handled by the CNF class
          // internally.
          yield(*cnf)->send(&socket(),
                            std::bind(&Connection::writeHandler,
                                      *this,
                                      boost::system::error_code(),
                                      0));
        } else {
          {
            std::ostream outStream(&sendStreambuf());
            cereal::BinaryOutputArchive oa(outStream);

            if((jd = std::get_if<messages::JobDescription>(
                  currentSendItem().get()))) {
              oa(*jd);
            } else if((msg = std::get_if<messages::Message>(
                         currentSendItem().get()))) {
              oa(*msg);
            }
          }

          sendSize() = boost::asio::buffer_size(sendStreambuf().data());
          yield async_write(socket(), BUF(sendSize), wh);

          PARACOOBA_LOG(logger(), NetTrace)
            << "Transmitting size " << sendSize();

          yield async_write(socket(), sendStreambuf(), wh);

          if((msg = std::get_if<messages::Message>(currentSendItem().get()))) {
            if(msg->getType() == messages::Type::OfflineAnnouncement) {
              PARACOOBA_LOG(logger(), NetTrace)
                << "This was an offline announcement, connection ends.";
              if(remoteNN()) {
                remoteNN()->resetConnection();
              }
              yield break;
            }
          }
        }
        currentlySending() = false;
        currentSendFinishedCB()(true);
        currentSendItem().reset();
      }
    }
  } else {
    // Error during writing!
    PARACOOBA_LOG(logger(), NetTrace)
      << "Error during sending! Error: " << ec.message();
    currentSendFinishedCB()(false);
    if(remoteNN()) {
      remoteNN()->resetConnection();
    }
  }
}

#undef BUF

#include <boost/asio/unyield.hpp>

void
Connection::connect(NetworkedNode& nn)
{
  // The initial peer that started the connection should also re-start the
  // connection.
  resumeMode() = RestartAfterShutdown;
  remoteNN() = &nn;
  remoteId() = nn.getId();

  uint16_t allowedRetries = config()->getUint16(Config::ConnectionRetries);

  ++connectionTry();

  if(getConnectionTries() < allowedRetries) {
    PARACOOBA_LOG(logger(), NetTrace)
      << "Trying to connect to " << nn.getRemoteTcpEndpoint()
      << " (ID: " << nn.getId() << "), Try " << getConnectionTries();
    socket().async_connect(
      nn.getRemoteTcpEndpoint(),
      boost::bind(
        &Connection::readHandler, *this, boost::asio::placeholders::error, 0));
  } else {
    PARACOOBA_LOG(logger(), NetTrace)
      << "Retries exhausted, no reconnect will be tried.";
    resumeMode() = EndAfterShutdown;
  }
}

Connection::Mode
Connection::getSendModeFromSendItem(const Connection::SendItem& sendItem)
{
  switch(sendItem.index()) {
    case 0:
      return Mode::TransmitCNF;
    case 1:
      return Mode::TransmitControlMessage;
    case 2:
      return Mode::TransmitJobDescription;
    case 3:
      return Mode::TransmitEndToken;
  }
  return Mode::TransmitModeUnknown;
}

bool
Connection::getLocalCNF()
{
  if(config()->isDaemonMode()) {
    auto [ctx, inserted] =
      config()->getDaemon()->getOrCreateContext(remoteId());
    context() = &ctx;
    cnf() = context()->getRootCNF();
  } else {
    PARACOOBA_LOG(logger(), LocalWarning)
      << "ReadBody should only ever be called on a daemon!";
    return false;
  }
  return true;
}

void
Connection::receiveIntoCNF(size_t bytes_received)
{
  recvStreambuf().commit(bytes_received);

  std::stringstream ssOut;
  boost::asio::streambuf::const_buffers_type constBuffer =
    recvStreambuf().data();

  std::copy(boost::asio::buffers_begin(constBuffer),
            boost::asio::buffers_begin(constBuffer) + bytes_received,
            std::ostream_iterator<uint8_t>(ssOut));

  recvStreambuf().consume(bytes_received);

  cnf()->receive(&socket(), ssOut.str().c_str(), bytes_received);
}

void
Connection::receiveSerializedMessage(size_t bytes_received)
{
  recvStreambuf().commit(bytes_received);

  std::istream inStream(&recvStreambuf());

  try {
    cereal::BinaryInputArchive ia(inStream);

    if(currentMode() == TransmitJobDescription) {
      messages::JobDescription jd;
      ia(jd);
      auto receiver =
        jobDescriptionReceiverProvider().getJobDescriptionReceiver(
          jd.getOriginatorID());
      if(receiver) {
        receiver->receiveJobDescription(remoteId(), std::move(jd), *remoteNN());

        if(context()) {
          // This is a connection between a Master and a Daemon. So the context
          // for this formula can be notified of this JD.
          switch(jd.getKind()) {
            case messages::JobDescription::Initiator:
              context()->start(Daemon::Context::State::AllowanceMapReceived);
              break;
            default:
              if(!context()->getReadyForWork()) {
                PARACOOBA_LOG(logger(), GlobalError)
                  << "Receive JobDescription while context is not ready for "
                     "work!";
              }
              break;
          }
        }
      } else {
        PARACOOBA_LOG(logger(), GlobalError)
          << "Received a JobDescription from " << remoteId()
          << " but no JD Receiver for originator " << jd.getOriginatorID()
          << " could be retrieved!";
      }
    } else if(currentMode() == TransmitControlMessage) {
      messages::Message msg;
      ia(msg);
      messageReceiver().receiveMessage(msg, *remoteNN());

      if(msg.getType() == messages::Type::OfflineAnnouncement) {
        resumeMode() = EndAfterShutdown;
      }
    }
  } catch(const cereal::Exception& e) {
    PARACOOBA_LOG(logger(), GlobalError)
      << "Exception during parsing of serialized message! Receive mode: "
      << currentMode() << ", Error: " << e.what();
    return;
  }
}

Logger&
Connection::logger()
{
  enrichLogger();
  return m_state->logger;
}

void
Connection::enrichLogger()
{
  if(!m_state->log->isLogLevelEnabled(Log::NetTrace))
    return;

  std::stringstream context;
  context << "{";
  context << "R:'" << std::to_string(getRemoteId()) << "',";
  context << "TX:'" << sendMode() << "',";
  context << "RX:'" << currentMode() << "'";
  context << "}";

  m_state->logger.setMeta(context.str());
}

void
Connection::popNextSendItem()
{
  assert(!currentSendItem());
  std::lock_guard lock(sendQueueMutex());
  if(sendQueue().empty())
    return;
  auto& queueFrontEntry = sendQueue().front();
  currentSendItem() = std::move(queueFrontEntry.sendItem);
  currentSendFinishedCB() = queueFrontEntry.sendFinishedCB;
  sendQueue().pop();
  writeHandler();
}

bool
Connection::initRemoteNN()
{
  if(remoteNN()) {
    // If the remote was already set from outside, this can return immediately.
    // No new node needs to be created.
    remoteNN()->assignConnection(*this);
    return true;
  }

  auto [node, inserted] = clusterNodeStore().getOrCreateNode(remoteId());
  remoteNN() = node.getNetworkedNode();

  enrichLogger();
  if(remoteNN()->assignConnection(*this)) {
    PARACOOBA_LOG(logger(), NetTrace)
      << "Initiated Remote NN, connection established.";
    return true;
  } else {
    PARACOOBA_LOG(logger(), NetTrace)
      << "Initiated Remote NN, but connection not required, as it was already "
         "initialized previously. Connection is dropped.";
    return false;
  }
}

std::ostream&
operator<<(std::ostream& o, Connection::Mode mode)
{
  return o << ConnectionModeToStr(mode);
}

std::ostream&
operator<<(std::ostream& o, Connection::ResumeMode resumeMode)
{
  return o << ConnectionResumeModeToStr(resumeMode);
}
}
}

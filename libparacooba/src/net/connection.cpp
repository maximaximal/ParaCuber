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
#include <boost/asio/error.hpp>
#include <boost/asio/placeholders.hpp>
#include <chrono>
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
  , resolver(ioService)
  , steadyTimer(ioService)
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
  if(remoteNN) {
    remoteNN->removeActiveTCPClient();
    remoteNN->resetConnection();
  }

  if(!exit && !config->isStopping()) {
    PARACOOBA_LOG(logger, NetTrace)
      << "Connection Ended with resume mode " << resumeMode;

    switch(resumeMode) {
      case RestartAfterShutdown: {
        // Attempt restart.
        if(remoteNN || remote != "") {
          Connection conn(ioService,
                          log,
                          config,
                          clusterNodeStore,
                          messageReceiver,
                          jobDescriptionReceiverProvider,
                          connectionTry);

          if(remoteNN) {
            conn.connect(*remoteNN);
          } else {
            conn.connect(remote);
          }

          // Also apply all old queue entries to send.
          while(!sendQueue.empty()) {
            auto entry = sendQueue.front();
            sendQueue.pop();
            conn.sendSendQueueEntry(std::move(entry));
          }
        } else {
          PARACOOBA_LOG(logger, NetTrace)
            << "Cannot restart connection, as no remoteNN is set and no remote "
               "address was specified!";
        }
        break;
      }
      case EndAfterShutdown:
        // Entries could not be sent, so notify callbacks.
        while(!sendQueue.empty()) {
          auto entry = sendQueue.front();
          sendQueue.pop();
          if(entry.sendFinishedCB) {
            entry.sendFinishedCB(false);
          }
        }
        break;
    }
  }
}

Connection::SendQueueEntry::SendQueueEntry()
  : sendFinishedCB(EmptySuccessCB)
{}
Connection::SendQueueEntry::SendQueueEntry(const SendQueueEntry& o)
  : sendItem(std::make_unique<SendItem>(*o.sendItem))
  , sendFinishedCB(o.sendFinishedCB)
{}
Connection::SendQueueEntry::SendQueueEntry(std::shared_ptr<CNF> cnf,
                                           const SuccessCB& sendFinishedCB)
  : sendItem(std::make_unique<SendItem>(cnf))
  , sendFinishedCB(sendFinishedCB)
{}
Connection::SendQueueEntry::SendQueueEntry(const messages::Message& msg,
                                           const SuccessCB& sendFinishedCB)
  : sendItem(std::make_unique<SendItem>(msg))
  , sendFinishedCB(sendFinishedCB)
{}
Connection::SendQueueEntry::SendQueueEntry(const messages::JobDescription& jd,
                                           const SuccessCB& sendFinishedCB)
  : sendItem(std::make_unique<SendItem>(jd))
  , sendFinishedCB(sendFinishedCB)
{}
Connection::SendQueueEntry::SendQueueEntry(EndTokenTag endTokenTag,
                                           const SuccessCB& sendFinishedCB)
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
  if(!isExit() && !config()->isStopping()) {
    if(sendMode() != TransmitEndToken && m_state.use_count() == 1) {
      // This is the last connection, so this is also the last one having a
      // reference to the state. This is a clean shutdown of a connection, the
      // socket will be ended. To differentiate between this clean shutdown and
      // a dirty one, an EndToken must be transmitted. This is the last action.
      Connection(*this).sendEndToken();
    }
  }
}

void
Connection::sendCNF(std::shared_ptr<CNF> cnf, const SuccessCB& sendFinishedCB)
{
  sendSendQueueEntry(SendQueueEntry(cnf, sendFinishedCB));
}
void
Connection::sendMessage(const messages::Message& msg,
                        const SuccessCB& sendFinishedCB)
{
  sendSendQueueEntry(SendQueueEntry(msg, sendFinishedCB));
}
void
Connection::sendJobDescription(const messages::JobDescription& jd,
                               const SuccessCB& sendFinishedCB)
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
      if(socket().available() >= sizeof(thisId())) {
        read(socket(), BUF(remoteId));
        yield async_write(socket(), BUF(thisId), rh);
        currentlySending() = false;
        writeHandler();
      } else {
        yield async_write(socket(), BUF(thisId), rh);
        currentlySending() = false;
        writeHandler();
        yield async_read(socket(), BUF(remoteId), rh);
      }

      if(remoteId() == thisId()) {
        PARACOOBA_LOG(logger(), NetTrace)
          << "Connection from same local ID, not accepting.";
        resumeMode() = EndAfterShutdown;
        yield break;
      }

      if(remoteId() == 0) {
        PARACOOBA_LOG(logger(), NetTrace)
          << "Remote ID is 0! Ending connection.";
        resumeMode() = EndAfterShutdown;
        yield break;
      }

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
      PARACOOBA_LOG(logger(), NetTrace)
        << "Connection ended! Retry after timeout." << ec.message();
      reconnectAfterMS(config()->getUint32(Config::NetworkTimeout));
    }
  } else if(ec == boost::asio::error::connection_refused) {
    PARACOOBA_LOG(logger(), NetTrace)
      << "Connection refused! Retry after short timeout." << ec.message();
    reconnectAfterMS(config()->getUint32(Config::ShortNetworkTimeout));
  } else {
    PARACOOBA_LOG(logger(), NetTrace)
      << "Error in readHandler: " << ec.message();
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

          PARACOOBA_LOG(logger(), NetTrace)
            << "Now sending CNF with size " << BytePrettyPrint(sendSize())
            << ".";

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
            << "Now sending message of type " << sendMode() << " with size "
            << BytePrettyPrint(sendSize()) << ".";

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

        if(currentSendFinishedCB()) {
          currentSendFinishedCB()(true);
        }
        currentSendItem().reset();
        currentlySending() = false;
      }
    }
  } else {
    // Error during writing!
    PARACOOBA_LOG(logger(), NetTrace)
      << "Error during sending! Error: " << ec.message();
    if(currentSendFinishedCB()) {
      currentSendFinishedCB()(false);
    }
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

void
Connection::connect(const std::string& remote)
{
  resumeMode() = EndAfterShutdown;
  this->remote() = remote;

  std::string host = remote;
  std::string port = std::to_string(config()->getUint16(Config::UDPTargetPort));

  auto posOfColon = remote.find(":");
  if(posOfColon != std::string::npos) {
    host = remote.substr(0, posOfColon);
    port = remote.substr(posOfColon + 1, std::string::npos);
  }

  uint16_t allowedRetries = config()->getUint16(Config::ConnectionRetries);
  ++connectionTry();

  if(getConnectionTries() < allowedRetries) {
    PARACOOBA_LOG(logger(), NetTrace)
      << "Trying to resolve " << remote << " (\"" << host << "\":\"" << port
      << "\"), Try " << connectionTry();

    Connection thisConn(*this);

    resolver().async_resolve(
      boost::asio::ip::tcp::resolver::query{ host, port },
      [*this, host, port, remote](
        const boost::system::error_code& ec,
        boost::asio::ip::tcp::resolver::iterator endpointIt) mutable {
        if(ec) {
          PARACOOBA_LOG(logger(), NetTrace)
            << "Could not resolve " << remote << " (\"" << host << "\":\""
            << port << "\")! Error: " << ec.message();
          return;
        }

        boost::asio::ip::tcp::endpoint chosenEndpoint;
        std::for_each(endpointIt,
                      {},
                      [*this, &chosenEndpoint, &host, &port, &remote](
                        auto& endpointEntry) mutable {
                        auto endpoint = endpointEntry.endpoint();
                        PARACOOBA_LOG(logger(), NetTrace)
                          << "Resolved " << remote << " (\"" << host << "\":\""
                          << port << "\") to endpoint " << endpoint
                          << " on try " << connectionTry();
                        chosenEndpoint = endpoint;
                      });

        PARACOOBA_LOG(logger(), NetTrace)
          << "Resolved " << remote << " (\"" << host << "\":\"" << port
          << "\") to endpoint " << chosenEndpoint << " on try "
          << connectionTry()
          << " which was chosen for connection. Now connecting.";

        socket().async_connect(chosenEndpoint,
                               boost::bind(&Connection::readHandler,
                                           *this,
                                           boost::asio::placeholders::error,
                                           0));
      });
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
        receiver->receiveJobDescription(std::move(jd), *remoteNN());

        if(context()) {
          // This is a connection between a Master and a Daemon. So the
          // context for this formula can be notified of this JD.
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
  context << "RID:'" << std::to_string(getRemoteId()) << "'";

  if(remote() != "") {
    context << ",R:'" << remote() << "'";
  } else if(socket().is_open()) {
    context << ",R:'" << socket().remote_endpoint() << "'";
  }

  if(socket().is_open()) {
    context << ",L:'" << socket().local_endpoint() << "'";
  }

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
  m_state->currentSendFinishedCB.swap(queueFrontEntry.sendFinishedCB);
  sendQueue().pop();
  writeHandler();
}

bool
Connection::initRemoteNN()
{
  if(remoteNN()) {
    // If the remote was already set from outside, this can return
    // immediately. No new node needs to be created.
    remoteNN()->assignConnection(*this);
    return true;
  }

  auto [node, inserted] = clusterNodeStore().getOrCreateNode(remoteId());

  if(node.getNetworkedNode()->assignConnection(*this)) {
    remoteNN() = node.getNetworkedNode();

    PARACOOBA_LOG(logger(), NetTrace)
      << "Initiated Remote NN, connection established.";

    if(inserted) {
      if(remote() != "") {
        // The side with specified remote is the one that originally started the
        // connection. This makes it also the side that now sends an
        // announcement request.
        remoteNN()->announcementRequest(*config(), *remoteNN());
      }
    }
    return true;
  } else {
    return false;
  }
}

void
Connection::reconnect()
{
  socket().close();

  if(remoteNN()) {
    connect(*remoteNN());
  } else if(remote() != "") {
    connect(remote());
  } else {
    PARACOOBA_LOG(logger(), NetTrace)
      << "Reconnect cancelled, as there is no remote defined.";
  }
}

void
Connection::reconnectAfterMS(uint32_t milliseconds)
{
  steadyTimer().expires_from_now(std::chrono::milliseconds(milliseconds));
  steadyTimer().async_wait(
    [*this](const boost::system::error_code& ec) mutable { reconnect(); });
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

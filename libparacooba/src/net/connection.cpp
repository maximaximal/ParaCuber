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

#ifdef PARACOOBA_ENABLE_TRACING_SUPPORT
#include "../../include/paracooba/tracer.hpp"
#include "../../include/paracooba/tracer_conversions.hpp"
#endif

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
  if(auto nn = remoteNN.lock()) {
    nn->removeActiveTCPClient();
    nn->resetConnection();
  }

  if(!exit && !config->isStopping()) {
    PARACOOBA_LOG(logger, NetDebug)
      << "Connection Ended with resume mode " << resumeMode;

    switch(resumeMode) {
      case RestartAfterShutdown: {
        // Attempt restart.
        Connection conn(ioService,
                        log,
                        config,
                        clusterNodeStore,
                        messageReceiver,
                        jobDescriptionReceiverProvider,
                        connectionTry);

        // Also apply all old queue entries to send.
        while(!sendQueue.empty()) {
          auto entry = sendQueue.front();
          sendQueue.pop();
          conn.sendSendQueueEntry(std::move(entry));
        }

        if(auto nn = remoteNN.lock()) {
          conn.connect(nn);
        } else if(remote != "") {
          conn.connect(remote);
        } else {
          PARACOOBA_LOG(logger, NetDebug)
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

  if(auto nn = remoteNN().lock()) {
    if(nn->deletionRequested()) {
      return;
    }
  }

  if(!ec) {
    reenter(&readCoro())
    {
      if(socket().available() >= sizeof(thisId())) {
        read(socket(), BUF(remoteId));
        yield async_write(socket(), BUF(thisId), rh);
        currentlySending() = false;
      } else {
        yield async_write(socket(), BUF(thisId), rh);
        currentlySending() = false;
        yield async_read(socket(), BUF(remoteId), rh);
      }

      if(remoteId() == thisId()) {
        PARACOOBA_LOG(logger(), NetDebug)
          << "Connection from same local ID, not accepting.";
        resumeMode() = EndAfterShutdown;
        yield break;
      }

      if(remoteId() == 0) {
        PARACOOBA_LOG(logger(), NetDebug)
          << "Remote ID is 0! Ending connection.";
        resumeMode() = EndAfterShutdown;
        yield break;
      }
      if(!initRemoteNN()) {
        // Exit coroutine and connection. This connection is already established
        // and is full duplex, the other direction is not required!
        PARACOOBA_LOG(logger(), NetDebug)
          << "Connection is already established from other side, connection "
             "will be dropped.";
        yield break;
      }

      if(auto nn = remoteNN().lock()) {
        nn->addActiveTCPClient();
      } else {
        PARACOOBA_LOG(logger(), LocalError)
          << "Connection established, remoteNN initialized, but no remoteNN "
             "can be transformed into shared_ptr! Connection will be "
             "dropped.";
        return;
      }
      connectionEstablished() = true;
      // should not be required and may lead to issues.
      // writeHandler();
#ifdef PARACOOBA_ENABLE_TRACING_SUPPORT
      {
        auto address = socket().remote_endpoint().address();

        if(address.is_v6()) {
          Tracer::log(-1,
                      traceentry::ConnectionEstablished{
                        remoteId(),
                        0,
                        address.to_v6().to_bytes(),
                        socket().remote_endpoint().port() });
        } else {
          Tracer::log(-1,
                      traceentry::ConnectionEstablished{
                        remoteId(),
                        address.to_v4().to_ulong(),
                        { 0 },
                        socket().remote_endpoint().port() });
        }
      }
#endif

      // Now set the connection to ready in the networked node.
      if(auto nn = remoteNN().lock()) {
        nn->getConnectionReadyWaiter().setReady(nn->getConnection());
      }

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

#ifdef PARACOOBA_ENABLE_TRACING_SUPPORT
          Tracer::log(
            cnf()->getOriginId(),
            traceentry::RecvMsg{
              remoteId(), size(), false, traceentry::MessageKind::CNF });
#endif

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

#ifdef PARACOOBA_ENABLE_TRACING_SUPPORT
    auto address = socket().remote_endpoint().address();

    if(address.is_v6()) {
      Tracer::log(
        -1,
        traceentry::ConnectionDropped{ remoteId(),
                                       0,
                                       address.to_v6().to_bytes(),
                                       socket().remote_endpoint().port() });
    } else {
      Tracer::log(
        -1,
        traceentry::ConnectionDropped{ remoteId(),
                                       address.to_v4().to_ulong(),
                                       { 0 },
                                       socket().remote_endpoint().port() });
    }
#endif

    if(resumeMode() == RestartAfterShutdown) {
      PARACOOBA_LOG(logger(), LocalWarning)
        << "Connection ended! Retry after timeout." << ec.message();
      reconnectAfterMS(config()->getUint32(Config::NetworkTimeout));
    }
  } else if(ec == boost::asio::error::connection_refused) {
    PARACOOBA_LOG(logger(), LocalError)
      << "Connection refused! Retry after short timeout." << ec.message();
    reconnectAfterMS(config()->getUint32(Config::ShortNetworkTimeout));
  } else {
    PARACOOBA_LOG(logger(), LocalError)
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
        while(!connectionEstablished()) {
          yield;
        }

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

          // Re-get CNF after yield.
          cnf = &std::get<std::shared_ptr<CNF>>(*currentSendItem());

          PARACOOBA_LOG(logger(), NetTrace)
            << "Now sending CNF with size " << BytePrettyPrint(sendSize())
            << ".";

#ifdef PARACOOBA_ENABLE_TRACING_SUPPORT
          Tracer::log(
            (*cnf)->getOriginId(),
            traceentry::SendMsg{
              remoteId(), sendSize(), false, traceentry::MessageKind::CNF });
#endif

          // Let sending of potentially huge CNF be handled by the CNF class
          // internally.
          assert(*cnf);
          yield(*cnf)->send(&socket(),
                            std::bind(&Connection::writeHandler,
                                      *this,
                                      boost::system::error_code(),
                                      0));
        } else {
          {
            std::ostream outStream(&sendStreambuf());
            cereal::BinaryOutputArchive oa(outStream);

#ifdef PARACOOBA_ENABLE_TRACING_SUPPORT
            traceentry::MessageKind messageKind =
              traceentry::MessageKind::Unknown;
            ID originID = -1;
#endif

            if((jd = std::get_if<messages::JobDescription>(
                  currentSendItem().get()))) {
              oa(*jd);
#ifdef PARACOOBA_ENABLE_TRACING_SUPPORT
              originID = jd->getOriginatorID();
              messageKind = JobDescriptionKindToTraceMessageKind(jd->getKind());
#endif
            } else if((msg = std::get_if<messages::Message>(
                         currentSendItem().get()))) {
              oa(*msg);
#ifdef PARACOOBA_ENABLE_TRACING_SUPPORT
              messageKind = MessageTypeToTraceMessageKind(msg->getType());
#endif
            }

            sendSize() = boost::asio::buffer_size(sendStreambuf().data());

#ifdef PARACOOBA_ENABLE_TRACING_SUPPORT
            Tracer::log(originID,
                        traceentry::SendMsg{
                          remoteId(), sendSize(), false, messageKind });
#endif
          }

          yield async_write(socket(), BUF(sendSize), wh);

          PARACOOBA_LOG(logger(), NetTrace)
            << "Now sending message of type " << sendMode() << " with size "
            << BytePrettyPrint(sendSize()) << ".";

          yield async_write(socket(), sendStreambuf(), wh);

          if((msg = std::get_if<messages::Message>(currentSendItem().get()))) {
            if(msg->getType() == messages::Type::OfflineAnnouncement) {
              PARACOOBA_LOG(logger(), NetTrace)
                << "This was an offline announcement, connection ends.";
              if(auto nn = remoteNN().lock()) {
                nn->resetConnection();
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
    PARACOOBA_LOG(logger(), LocalError)
      << "Error during sending! Error: " << ec.message();
    if(currentSendFinishedCB()) {
      currentSendFinishedCB()(false);
    }
    if(auto nn = remoteNN().lock()) {
      nn->resetConnection();
    }
  }
}

#undef BUF

#include <boost/asio/unyield.hpp>

void
Connection::connect(const NetworkedNodePtr& nn)
{
  // The initial peer that started the connection should also re-start the
  // connection.
  resumeMode() = RestartAfterShutdown;
  remoteNN() = nn;
  remoteId() = nn->getId();

  uint16_t allowedRetries = config()->getUint16(Config::ConnectionRetries);

  ++connectionTry();

  if(getConnectionTries() < allowedRetries) {
    PARACOOBA_LOG(logger(), NetDebug)
      << "Trying to connect to " << nn->getRemoteTcpEndpoint()
      << " (ID: " << nn->getId() << "), Try " << getConnectionTries();
    socket().async_connect(
      nn->getRemoteTcpEndpoint(),
      boost::bind(
        &Connection::readHandler, *this, boost::asio::placeholders::error, 0));
  } else {
    PARACOOBA_LOG(logger(), NetDebug)
      << "Retries exhausted, no reconnect will be tried.";
    resumeMode() = EndAfterShutdown;
  }
}

void
Connection::connect(const std::string& remote)
{
  std::string host = remote;
  std::string port = std::to_string(config()->getUint16(Config::TCPTargetPort));

  auto posOfColon = remote.find(":");
  if(posOfColon != std::string::npos) {
    host = remote.substr(0, posOfColon);
    port = remote.substr(posOfColon + 1, std::string::npos);
  }

  std::string connectionString = host + ":" + port;

  if(clusterNodeStore().remoteConnectionStringKnown(connectionString)) {
    PARACOOBA_LOG(logger(), NetDebug)
      << "Remote connection string \"" << remote
      << "\" already known! Ending connection on connection try "
      << getConnectionTries();
    return;
  }

  resumeMode() = EndAfterShutdown;
  this->remote() = connectionString;

  uint16_t allowedRetries = config()->getUint16(Config::ConnectionRetries);
  ++connectionTry();

  if(getConnectionTries() < allowedRetries) {
    PARACOOBA_LOG(logger(), NetDebug)
      << "Trying to resolve " << remote << " (\"" << host << "\":\"" << port
      << "\"), Try " << connectionTry();

    Connection thisConn(*this);

    resolver().async_resolve(
      boost::asio::ip::tcp::resolver::query{ host, port },
      [*this, host, port, remote](
        const boost::system::error_code& ec,
        boost::asio::ip::tcp::resolver::iterator endpointIt) mutable {
        if(ec) {
          PARACOOBA_LOG(logger(), LocalError)
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
                        PARACOOBA_LOG(logger(), NetDebug)
                          << "Resolved " << remote << " (\"" << host << "\":\""
                          << port << "\") to endpoint " << endpoint
                          << " on try " << connectionTry();
                        chosenEndpoint = endpoint;
                      });

        PARACOOBA_LOG(logger(), NetDebug)
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
        if(auto nn = remoteNN().lock()) {
          auto kind = jd.getKind();

#ifdef PARACOOBA_ENABLE_TRACING_SUPPORT
          Tracer::log(
            jd.getOriginatorID(),
            traceentry::RecvMsg{ remoteId(),
                                 bytes_received,
                                 false,
                                 JobDescriptionKindToTraceMessageKind(kind) });
#endif

          receiver->receiveJobDescription(std::move(jd), nn);

          if(context()) {
            // This is a connection between a Master and a Daemon. So the
            // context for this formula can be notified of this JD.
            switch(kind) {
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
          PARACOOBA_LOG(logger(), LocalError)
            << "Received a JobDescription from " << remoteId()
            << " but no remoteNN in connection!";
        }
      } else {
        PARACOOBA_LOG(logger(), GlobalError)
          << "Received a JobDescription from " << remoteId()
          << " but no JD Receiver for originator " << jd.getOriginatorID()
          << " could be retrieved!";
      }
    } else if(currentMode() == TransmitControlMessage) {
      if(auto nn = remoteNN().lock()) {
        messages::Message msg;
        ia(msg);

#ifdef PARACOOBA_ENABLE_TRACING_SUPPORT
        Tracer::log(
          -1,
          traceentry::RecvMsg{ remoteId(),
                               bytes_received,
                               false,
                               MessageTypeToTraceMessageKind(msg.getType()) });
#endif

        messageReceiver().receiveMessage(msg, *nn);

        if(msg.getType() == messages::Type::OfflineAnnouncement) {
          resumeMode() = EndAfterShutdown;
        }
      } else {
        PARACOOBA_LOG(logger(), LocalError)
          << "Received a Serialized Message from " << remoteId()
          << " but no remoteNN in connection!";
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
  if(!m_state->log->isLogLevelEnabled(Log::NetTrace) &&
     !m_state->log->isLogLevelEnabled(Log::NetDebug))
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
  std::lock_guard lock(sendQueueMutex());
  if(currentSendItem()) {
    // This may happen if called from multiple threads.
    return;
  }
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
  if(auto nn = remoteNN().lock()) {
    // If the remote was already set from outside, this can return
    // immediately. No new node needs to be created.
    nn->assignConnection(*this);
    return true;
  }

  auto [node, inserted] = clusterNodeStore().getOrCreateNode(remoteId());

  if(node.getNetworkedNode()->assignConnection(*this)) {
    auto nn = node.getNetworkedNodePtr();
    remoteNN() = nn;

    PARACOOBA_LOG(logger(), Trace)
      << "Initiated Remote NN, connection established.";

    if(inserted) {
      if(remote() != "") {
        // The side with specified remote is the one that originally started the
        // connection. This makes it also the side that now sends an
        // announcement request.
        //
        // The message must be built locally and not be sent using the
        // NetworkedNode, because the networked node is to be activated outside
        // of this function.
        //
        // Before sending the announcement request, which immediately would
        // trigger a CNF to be sent from clients, a ping message is sent. This
        // determines the connection latency between the two peers. This must
        // also be registered in the message receiver, so it can track when the
        // ping has been transmitted and the answer received.
        messageReceiver().handlePingSent(remoteId());
        messages::Message pingMessage(config()->getId());
        pingMessage.insert(messages::Ping(remoteId()));
        sendMessage(pingMessage);

        messages::Message msg(config()->getId());
        msg.insert(messages::AnnouncementRequest(config()->buildNode()));
        sendMessage(msg);
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

  if(auto nn = remoteNN().lock()) {
    connect(nn);
  } else if(remote() != "") {
    connect(remote());
  } else {
    PARACOOBA_LOG(logger(), NetDebug)
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

boost::asio::ip::tcp::endpoint
Connection::getRemoteTcpEndpoint() const
{
  return socket().remote_endpoint();
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

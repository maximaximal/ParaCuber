#include "../../include/paracooba/net/connection.hpp"
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

Connection::State::State(boost::asio::io_service& ioService,
                         LogPtr log,
                         ConfigPtr config,
                         ClusterNodeStore& clusterNodeStore,
                         messages::MessageReceiver& msgRec,
                         messages::JobDescriptionReceiverProvider& jdRecProv)
  : ioService(ioService)
  , socket(ioService)
  , log(log)
  , logger(log->createLogger("Connection"))
  , config(config)
  , clusterNodeStore(clusterNodeStore)
  , messageReceiver(msgRec)
  , jobDescriptionReceiverProvider(jdRecProv)
{
  thisId = config->getInt64(Config::Id);
}

Connection::State::~State()
{
  PARACOOBA_LOG(logger, Trace)
    << "Connection Ended with resume mode " << resumeMode;
  switch(resumeMode) {
    case RestartAfterShutdown:
      break;
    case EndAfterShutdown:
      break;
  }
}

Connection::SendQueueEntry::SendQueueEntry() {}
Connection::SendQueueEntry::SendQueueEntry(const SendQueueEntry& o)
  : sendItem(std::make_unique<SendItem>(*o.sendItem))
{}
Connection::SendQueueEntry::SendQueueEntry(std::shared_ptr<CNF> cnf)
  : sendItem(std::make_unique<SendItem>(cnf))
{}
Connection::SendQueueEntry::SendQueueEntry(const messages::Message& msg)
  : sendItem(std::make_unique<SendItem>(msg))
{}
Connection::SendQueueEntry::SendQueueEntry(const messages::JobDescription& jd)
  : sendItem(std::make_unique<SendItem>(jd))
{}
Connection::SendQueueEntry::~SendQueueEntry() {}

Connection::Connection(boost::asio::io_service& ioService,
                       LogPtr log,
                       ConfigPtr config,
                       ClusterNodeStore& clusterNodeStore,
                       messages::MessageReceiver& msgRec,
                       messages::JobDescriptionReceiverProvider& jdRecProv)
  : m_state(std::make_shared<State>(ioService,
                                    log,
                                    config,
                                    clusterNodeStore,
                                    msgRec,
                                    jdRecProv))
{}
Connection::~Connection() {}

void
Connection::sendCNF(std::shared_ptr<CNF> cnf)
{
  sendSendQueueEntry(SendQueueEntry(cnf));
}
void
Connection::sendMessage(const messages::Message& msg)
{
  sendSendQueueEntry(SendQueueEntry(msg));
}
void
Connection::sendJobDescription(const messages::JobDescription& jd)
{
  sendSendQueueEntry(SendQueueEntry(jd));
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

  enrichLogger();

  if(!ec) {
    reenter(&readCoro())
    {
      yield async_write(socket(), BUF(thisId), rh);

      currentlySending() = false;
      // Write pending items from queue.
      writeHandler();

      yield async_read(socket(), BUF(remoteId), rh);

      connectionEstablished() = true;

      // Handshake finished, both sides know about the other side. Communication
      // can begin now. Communication runs in a loop, as unlimited messages may
      // be received over a connection.

      for(;;) {
        yield async_read(socket(), BUF(currentModeAsUint8), rh);

        if(currentMode() == TransmitCNF) {
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

  enrichLogger();

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

        if((cnf = std::get_if<std::shared_ptr<CNF>>(currentSendItem().get()))) {
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
            << "Transmit with mode " << sendMode() << " and size "
            << sendSize();

          yield async_write(socket(), sendStreambuf(), wh);
        }
        currentlySending() = false;
        currentSendItem().reset();
      }
    }
  } else {
    // Error during writing!
    PARACOOBA_LOG(logger(), NetTrace)
      << "Error during sending! Error: " << ec.message();
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

  uint16_t allowedRetries = config()->getUint16(Config::ConnectionRetries);

  if(getConnectionTries() < allowedRetries) {
    PARACOOBA_LOG(logger(), NetTrace)
      << "Trying to connect to " << nn.getRemoteTcpEndpoint()
      << "(ID: " << nn.getId() << "), Try " << getConnectionTries();
    ++connectionTry();
    socket().async_connect(
      nn.getRemoteTcpEndpoint(),
      boost::bind(
        &Connection::readHandler, *this, boost::asio::placeholders::error, 0));
  } else {
    PARACOOBA_LOG(logger(), NetTrace)
      << "Retries exhausted, no reconnect will be tried.";
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
    }
  } catch(const cereal::Exception& e) {
    PARACOOBA_LOG(logger(), GlobalError)
      << "Exception during parsing of serialized message! Receive mode: "
      << currentMode() << ", Error: " << e.what();
    return;
  }
}

void
Connection::enrichLogger()
{
  if(m_state->log->isLogLevelEnabled(Log::NetTrace)) {
    std::stringstream context;
    context << "{";

    context << "R:'" << remoteId() << ",";
    context << "TX:'" << sendMode() << "',";
    context << "RX:'" << currentMode() << "'";

    context << "}";

    logger() = m_state->log->createLogger("Connection", context.str());
  }
}

void
Connection::popNextSendItem()
{
  assert(!currentSendItem());
  std::lock_guard lock(sendQueueMutex());
  if(sendQueue().empty())
    return;
  currentSendItem() = std::move(sendQueue().front().sendItem);
  sendQueue().pop();
  writeHandler();
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

#ifndef PARACOOBA_NET_CONNECTION
#define PARACOOBA_NET_CONNECTION

#include <memory>

#include <boost/asio/io_service.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/streambuf.hpp>

#include "../daemon.hpp"
#include "../log.hpp"

namespace paracooba {
namespace messages {
class Message;
class JobDescription;
}

class CNF;

namespace net {
class Control;

class Connection : public std::enable_shared_from_this<Connection>
{
  public:
  enum Phase
  {
    Start,
    ReadRemoteID,
    ReadSubject,
    ReadBody,
    ReadJobDescriptionLength,
    ReadJobDescription,
    ReadMessageLength,
    ReadMessage,
    End,
  };

  Connection(boost::asio::io_service& ioService,
             LogPtr log,
             Control* control,
             Communicator* comm);
  virtual ~Connection();

  void sendCNF(std::shared_ptr<CNF> cnf);
  void sendMessage(const messages::Message& msg);
  void sendJobDescription(const messages::JobDescription& jd);

  private:
  boost::asio::io_service& m_ioService;
  boost::asio::ip::tcp::socket m_socket;
  boost::asio::streambuf m_streambuf;
  Logger m_logger;

  Communicator* m_comm;
  Control* m_control;

  Daemon::Context* m_context = nullptr;

  int64_t m_remoteID = 0;
};
}
}

#endif

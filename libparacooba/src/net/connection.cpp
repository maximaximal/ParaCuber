#include "../../include/paracooba/net/connection.hpp"

namespace paracooba {
namespace net {

Connection::Connection(boost::asio::io_service& ioService,
                       LogPtr log,
                       Control* control,
                       Communicator* comm)
  : m_ioService(ioService)
  , m_socket(ioService)
{}
Connection::~Connection() {}

void
Connection::sendCNF(std::shared_ptr<CNF> cnf)
{}
void
Connection::sendMessage(const messages::Message& msg)
{}
void
Connection::sendJobDescription(const messages::JobDescription& jd)
{}
}
}

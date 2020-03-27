#include "../include/paracooba/networked_node.hpp"
#include "../include/paracooba/net/connection.hpp"
#include <boost/type_traits/is_stateless.hpp>
#include <memory>

namespace paracooba {
NetworkedNode::NetworkedNode(
  ID id,
  messages::MessageTransmitter& statelessMessageTransmitter)
  : m_id(id)
  , m_statelessMessageTransmitter(statelessMessageTransmitter)
{}

NetworkedNode::~NetworkedNode() {}

void
NetworkedNode::transmitMessage(const messages::Message& msg,
                               NetworkedNode& nn,
                               SuccessCB sendFinishedCB)
{
  assert(&nn == this);
  if(m_connectionReadyWaiter.isReady()) {
    assert(m_connection);
    m_connection->sendMessage(msg, sendFinishedCB);
  } else {
    m_statelessMessageTransmitter.transmitMessage(msg, nn, sendFinishedCB);
  }
}

void
NetworkedNode::transmitJobDescription(messages::JobDescription&& jd,
                                      NetworkedNode& nn,
                                      SuccessCB sendFinishedCB)
{
  assert(&nn == this);
  assert(m_connectionReadyWaiter.isReady());
  assert(m_connection);
  m_connection->sendJobDescription(jd, sendFinishedCB);
}

void
NetworkedNode::requestDeletion()
{
  m_deletionRequested = true;
  if(m_connection) {
    m_connection->exit();
    resetConnection();
  }
}

bool
NetworkedNode::isConnectionReady() const
{
  return m_connectionReadyWaiter.isReady();
}

bool
NetworkedNode::assignConnection(const net::Connection& conn)
{
  if(m_connection) {
    return false;
  }
  m_connection = std::make_unique<net::Connection>(conn);
  m_connectionReadyWaiter.setReady(m_connection.get());
  return true;
}

void
NetworkedNode::resetConnection()
{
  m_connectionReadyWaiter.reset();
  m_connection.reset();
}
}

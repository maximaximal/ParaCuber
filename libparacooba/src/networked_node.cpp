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
}

void
NetworkedNode::transmitJobDescription(messages::JobDescription&& jd,
                                      NetworkedNode& nn,
                                      SuccessCB sendFinishedCB)
{
  assert(&nn == this);
}

bool
NetworkedNode::assignConnection(const net::Connection& conn)
{
  if(m_connection) {
    return false;
  }
  m_connection = std::make_unique<net::Connection>(conn);
  return true;
}
}

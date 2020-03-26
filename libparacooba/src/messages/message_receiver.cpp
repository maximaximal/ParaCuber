#include "../../include/paracooba/messages/message_receiver.hpp"
#include "../../include/paracooba/config.hpp"
#include "../../include/paracooba/messages/message.hpp"
#include "../../include/paracooba/networked_node.hpp"

namespace paracooba {
namespace messages {

void
MessageTransmitter::onlineAnnouncement(const Config& config,
                                       NetworkedNode& nn,
                                       SuccessCB sendFinishedCB)
{
  messages::Message msg = buildMessage(config);
  msg.insert(messages::OnlineAnnouncement(config.buildNode()));
  nn.transmitMessage(msg, nn, sendFinishedCB);
}

void
MessageTransmitter::offlineAnnouncement(const Config& config,
                                        NetworkedNode& nn,
                                        const std::string& reason,
                                        SuccessCB sendFinishedCB)
{
  messages::Message msg = buildMessage(config);
  msg.insert(messages::OfflineAnnouncement(reason));
  nn.transmitMessage(msg, nn, sendFinishedCB);
}

void
MessageTransmitter::announcementRequest(const Config& config,
                                        NetworkedNode& nn,
                                        SuccessCB sendFinishedCB)
{
  messages::Message msg = buildMessage(config);
  msg.insert(messages::AnnouncementRequest());
  nn.transmitMessage(msg, nn, sendFinishedCB);
}

messages::Message
MessageTransmitter::buildMessage(const Config& config)
{
  return std::move(messages::Message(config.getId()));
}
}
}

#include "../../include/paracooba/messages/message_receiver.hpp"
#include "../../include/paracooba/config.hpp"
#include "../../include/paracooba/messages/message.hpp"

namespace paracooba {
namespace messages {

void
MessageTransmitter::onlineAnnouncement(const Config& config,
                                       NetworkedNode& nn,
                                       SuccessCB sendFinishedCB)
{
  messages::Message msg = buildMessage(config);
}

void
MessageTransmitter::offlineAnnouncement(const Config& config,
                                        NetworkedNode& nn,
                                        const std::string& reason,
                                        SuccessCB sendFinishedCB)
{}

messages::Message
MessageTransmitter::buildMessage(const Config& config)
{
  return std::move(messages::Message(config.getId()));
}
}
}

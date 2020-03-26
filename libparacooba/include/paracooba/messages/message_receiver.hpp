#ifndef PARACOOBA_MESSAGES_MESSAGE_RECEIVER
#define PARACOOBA_MESSAGES_MESSAGE_RECEIVER

#include <functional>

#include "../types.hpp"

namespace paracooba {
class NetworkedNode;
class Config;

namespace messages {
class Message;

class MessageReceiver
{
  public:
  virtual void receiveMessage(const Message& msg, NetworkedNode& nn) = 0;
};

class MessageTransmitter
{
  public:
  virtual void transmitMessage(const Message& jd,
                               NetworkedNode& nn,
                               SuccessCB sendFinishedCB = EmptySuccessCB) = 0;

  void onlineAnnouncement(const Config& cfg,
                          NetworkedNode& nn,
                          SuccessCB sendFinishedCB = EmptySuccessCB);

  void offlineAnnouncement(const Config& cfg,
                           NetworkedNode& nn,
                           const std::string& reason,
                           SuccessCB sendFinishedCB = EmptySuccessCB);

  messages::Message buildMessage(const Config& config);
};
}
}

#endif

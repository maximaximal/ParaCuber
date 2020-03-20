#ifndef PARACOOBA_MESSAGES_MESSAGE_RECEIVER
#define PARACOOBA_MESSAGES_MESSAGE_RECEIVER

#include <functional>

namespace paracooba {
class NetworkedNode;

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
                               std::function<void(bool)> sendFinishedCB) = 0;
};
}
}

#endif

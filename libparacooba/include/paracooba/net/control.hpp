#ifndef PARACOOBA_NET_CONTROL
#define PARACOOBA_NET_CONTROL

#include "../messages/message_receiver.hpp"

namespace paracooba {
class NetworkedNode;

namespace messages {
class Message;
}

namespace net {
class Connection;

class Control : public messages::MessageReceiver
{
  public:
  Control();
  ~Control();

  virtual void receiveMessage(const messages::Message& msg, NetworkedNode& nn);

  private:
  void handleOnlineAnnouncement(const messages::Message& msg,
                                NetworkedNode& conn);
  void handleOfflineAnnouncement(const messages::Message& msg,
                                 NetworkedNode& conn);
  void handleAnnouncementRequest(const messages::Message& msg,
                                 NetworkedNode& conn);
  void handleNodeStatus(const messages::Message& msg, NetworkedNode& conn);
  void handleCNFTreeNodeStatusRequest(const messages::Message& msg,
                                      NetworkedNode& conn);
  void handleCNFTreeNodeStatusReply(const messages::Message& msg,
                                    NetworkedNode& conn);
};
}
}

#endif

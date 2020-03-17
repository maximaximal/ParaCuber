#ifndef PARACOOBA_NET_CONTROL
#define PARACOOBA_NET_CONTROL

namespace paracooba {
namespace messages {
class Message;
}

namespace net {
class Control
{
  public:
  Control();
  ~Control();

  void receiveMessage(const messages::Message& msg);

  private:
  void handleOnlineAnnouncement(const messages::Message& msg);
  void handleOfflineAnnouncement(const messages::Message& msg);
  void handleAnnouncementRequest(const messages::Message& msg);
  void handleNodeStatus(const messages::Message& msg);
  void handleCNFTreeNodeStatusRequest(const messages::Message& msg);
  void handleCNFTreeNodeStatusReply(const messages::Message& msg);
};
}
}

#endif

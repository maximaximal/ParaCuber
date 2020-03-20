#include "../../include/paracooba/net/control.hpp"

namespace paracooba {
namespace net {
Control::Control() {}
Control::~Control() {}

void
Control::receiveMessage(const messages::Message& msg, NetworkedNode& conn)
{}

void
Control::handleOnlineAnnouncement(const messages::Message& msg,
                                  NetworkedNode& conn)

{}
void
Control::handleOfflineAnnouncement(const messages::Message& msg,
                                   NetworkedNode& conn)
{}
void
Control::handleAnnouncementRequest(const messages::Message& msg,
                                   NetworkedNode& conn)
{}
void
Control::handleNodeStatus(const messages::Message& msg, NetworkedNode& conn)
{}
void
Control::handleCNFTreeNodeStatusRequest(const messages::Message& msg,
                                        NetworkedNode& conn)
{}
void
Control::handleCNFTreeNodeStatusReply(const messages::Message& msg,
                                      NetworkedNode& conn)
{}
}
}

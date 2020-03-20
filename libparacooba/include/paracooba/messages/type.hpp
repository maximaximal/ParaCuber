#ifndef PARACOOBA_MESSAGES_TYPE
#define PARACOOBA_MESSAGES_TYPE

namespace paracooba {
namespace messages {
enum class Type
{
  AnnouncementRequest,
  OnlineAnnouncement,
  OfflineAnnouncement,
  NodeStatus,
  CNFTreeNodeStatusRequest,
  CNFTreeNodeStatusReply,
  Unknown
};

constexpr const char*
TypeToStr(Type type)
{
  switch(type) {
    case Type::AnnouncementRequest:
      return "AnnouncementRequest";
    case Type::OnlineAnnouncement:
      return "OnlineAnnouncement";
    case Type::OfflineAnnouncement:
      return "OfflineAnnouncement";
    case Type::NodeStatus:
      return "NodeStatus";
    case Type::CNFTreeNodeStatusRequest:
      return "CNFTreeNodeStatusRequest";
    case Type::CNFTreeNodeStatusReply:
      return "CNFTreeNodeStatusReply";
    case Type::Unknown:
      return "Unknown Type (Specified)";
  }
  return "Unknown Type";
}

inline std::ostream&
operator<<(std::ostream& o, Type t)
{
  return o << TypeToStr(t);
}
}
}

#endif

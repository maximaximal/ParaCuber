#ifndef PARACOOBA_COMMON_MESSAGE_KIND_H
#define PARACOOBA_COMMON_MESSAGE_KIND_H

#ifdef __cplusplus
extern "C" {
#endif

typedef enum parac_message_kind {
  PARAC_MESSAGE_UNKNOWN,
  PARAC_MESSAGE_CNF,
  PARAC_MESSAGE_ONLINE_ANNOUNCEMENT,
  PARAC_MESSAGE_OFFLINE_ANNOUNCEMENT,
  PARAC_MESSAGE_ANNOUNCEMENT_REQUEST,
  PARAC_MESSAGE_NODE_STATUS,
  PARAC_MESSAGE_CNF_TREE_NODE_STATUS_REQUEST,
  PARAC_MESSAGE_CNF_TREE_NODE_STATUS_REPLY,
  PARAC_MESSAGE_NEW_REMOTE_CONNECTED,
  PARAC_MESSAGE_PING,
  PARAC_MESSAGE_PONG,
  PARAC_MESSAGE_JOB_PATH,
  PARAC_MESSAGE_JOB_RESULT,
  PARAC_MESSAGE_JOB_INITIATOR,

  PARAC_MESSAGE_ACK
} parac_message_kind;

const char*
parac_message_kind_to_str(parac_message_kind kind);

#ifdef __cplusplus
}

#include <iostream>

std::ostream&
operator<<(std::ostream& o, parac_message_kind kind) {
  return o << parac_message_kind_parac_message_kind_to_str(kind);
}

#endif

#endif

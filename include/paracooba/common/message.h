#ifndef PARAC_COMMON_MESSAGE_H
#define PARAC_COMMON_MESSAGE_H

#include "message_kind.h"
#include "status.h"
#include "types.h"

#ifdef __cplusplus
extern "C" {
#endif

#include <stdbool.h>
#include <stddef.h>

struct parac_message;
struct parac_compute_node;

typedef void (*parac_message_cb)(struct parac_message*, parac_status);

typedef struct parac_message {
  parac_message_kind kind;
  char* data;
  bool data_to_be_freed;
  size_t length;
  void* userdata;
  parac_message_cb cb;
  struct parac_compute_node* origin;
  parac_id originator_id;
} parac_message;

#ifdef __cplusplus
}

class parac_message_wrapper : public parac_message {
  public:
  parac_message_wrapper() {
    kind = static_cast<parac_message_kind>(0);
    data = nullptr;
    length = 0;
    data_to_be_freed = false;
    userdata = 0;
    cb = nullptr;
    origin = 0;
    originator_id = 0;
  };
  parac_message_wrapper(const parac_message& msg) {
    kind = msg.kind;
    data = msg.data;
    length = msg.length;
    data_to_be_freed = msg.data_to_be_freed;
    userdata = msg.userdata;
    cb = msg.cb;
    origin = msg.origin;
    originator_id = msg.originator_id;
  }
  parac_message_wrapper(parac_message&& msg) noexcept {
    kind = msg.kind;
    data = msg.data;
    length = msg.length;
    data_to_be_freed = msg.data_to_be_freed;
    userdata = msg.userdata;
    cb = msg.cb;
    origin = msg.origin;
    originator_id = msg.originator_id;
  }
  ~parac_message_wrapper() = default;

  void doCB(parac_status status) {
    if(cb)
      cb(this, status);
  }
};
#endif

#endif

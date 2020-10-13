#ifndef PARACOOBA_COMMON_PATH_H
#define PARACOOBA_COMMON_PATH_H

#include "paracooba/common/status.h"

#ifdef __cplusplus
extern "C" {
#endif

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#define PARAC_PATH_MAX_LENGTH 58
#define PARAC_PATH_EXPLICITLY_UNKNOWN 0b00111110u
#define PARAC_PATH_BITS 58u
#define PARAC_PATH_LENGTH_BITS 6u

typedef uint64_t parac_path_type;
typedef uint8_t parac_path_length_type;

typedef struct __attribute__((__packed__)) parac_path {
  union __attribute__((__packed__)) {
    struct __attribute__((__packed__)) {
      parac_path_length_type length : 6;
      parac_path_type path : 58;
    };
    parac_path_type rep : 64;
  };
} parac_path;

parac_path_type
parac_path_left_aligned(parac_path p);

bool
parac_path_get_assignment(parac_path p, parac_path_length_type d);

parac_path
parac_path_set_assignment(parac_path p, parac_path_length_type d, bool v);

parac_path_type
parac_path_get_depth_shifted(parac_path p);

parac_path_type
parac_path_get_shifted(parac_path p);

parac_path
parac_path_get_parent(parac_path p);

parac_path
parac_path_get_sibling(parac_path p);

parac_path
parac_path_get_next_left(parac_path p);

parac_path
parac_path_get_next_right(parac_path p);

parac_path
parac_path_cleanup(parac_path p);

parac_status
parac_path_from_str(const char* str, size_t len, parac_path* tgt);

void
parac_path_to_str(parac_path p, char* out_str);

#ifdef __cplusplus
}
#include <iostream>
#include <string>

namespace paracooba {
class Path : public parac_path {

  public:
  using type = parac_path_type;
  using length_type = parac_path_length_type;

  Path() = default;
  Path(const Path& o) = default;
  Path(parac_path p) { rep = p.rep; }
  Path(parac_path_type p) { rep = p; }

  template<typename T>
  static Path build(T p, length_type l) {
    Path path;
    path.rep = p;
    path.rep <<= (((sizeof(Path)) - (sizeof(T))) * 8);
    path.length = l;
    path = parac_path_cleanup(path);
    return path;
  }

  Path left() const { return parac_path_get_next_left(*this); }
  Path right() const { return parac_path_get_next_right(*this); }
  Path parent() const { return parac_path_get_parent(*this); }
  Path sibling() const { return parac_path_get_sibling(*this); }
  type shifted() const { return parac_path_get_shifted(*this); }
  type depth_shifted() const { return parac_path_get_depth_shifted(*this); }
  type left_aligned() const { return parac_path_left_aligned(*this); }

  struct BoolWrapper {
    public:
    BoolWrapper(Path& path, length_type pos)
      : m_path(path)
      , m_pos(pos) {}
    operator bool() const { return parac_path_get_assignment(m_path, m_pos); }
    void operator=(bool val) {
      m_path = parac_path_set_assignment(m_path, m_pos, val);
    }

    private:
    Path& m_path;
    length_type m_pos;
  };

  BoolWrapper operator[](parac_path_length_type pos) {
    return BoolWrapper(*this, pos);
  }
  bool operator[](parac_path_length_type pos) const {
    return parac_path_get_assignment(*this, pos);
  }

  void operator=(parac_path p) { rep = p.rep; }
  operator uint64_t() const { return rep; }
  bool operator<(const Path& o) const { return length < o.length; }
};

inline std::string
to_string(Path const& p) {
  char str[PARAC_PATH_MAX_LENGTH];
  parac_path_to_str(p, str);
  return std::string(str);
}
}

inline std::ostream&
operator<<(std::ostream& o, parac_path p) {
  char str[PARAC_PATH_MAX_LENGTH];
  parac_path_to_str(p, str);
  return o << str;
}
#endif

#endif
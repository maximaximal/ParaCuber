#ifndef PARACOOBA_READYWAITER_HPP
#define PARACOOBA_READYWAITER_HPP

#include <atomic>
#include <cassert>
#include <functional>
#include <mutex>
#include <queue>

namespace paracooba {
template<typename T>
class ReadyWaiter
{
  public:
  ReadyWaiter() {}
  ~ReadyWaiter() {}

  using ReadyCB = std::function<void(T&)>;

  inline void reset()
  {
    m_ready = false;
    m_value = nullptr;
  }
  inline void setReady(T* value)
  {
    m_ready = true;
    m_value = value;
    std::lock_guard<std::mutex> lock(m_queueMutex);
    while(!m_readyCBs.empty()) {
      m_readyCBs.front()(*m_value);
      m_readyCBs.pop();
    };
  }
  inline bool isReady() const { return m_ready; }
  inline void callWhenReady(ReadyCB cb)
  {
    if(isReady()) {
      cb(*m_value);
    } else {
      std::unique_lock<std::mutex> lock(m_queueMutex);
      m_readyCBs.push(cb);
    }
  }

  private:
  std::mutex m_queueMutex;
  std::atomic<bool> m_ready = false;
  T* m_value = nullptr;
  std::queue<ReadyCB> m_readyCBs;
};
}

#endif

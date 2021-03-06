#pragma once

#include <functional>
#include <memory>
#include <string>

#include "paracooba/common/types.h"
#include <paracooba/common/status.h>
#include <paracooba/common/task.h>

struct parac_task;
struct parac_handle;

namespace parac::solver {
class CaDiCaLHandle;

class ParserTask {
  public:
  using CaDiCaLHandlePtr = std::unique_ptr<CaDiCaLHandle>;
  using FinishedCB = std::function<void(parac_status, CaDiCaLHandlePtr)>;

  ParserTask(parac_handle& handle,
             parac_task& task,
             std::string input,
             parac_id originatorId,
             FinishedCB finishedCB);
  ~ParserTask();

  private:
  struct Internal;
  std::unique_ptr<Internal> m_internal;

  static parac_status work(parac_task* self, parac_worker worker);

  parac_task& m_task;
  const std::string m_input;
  std::string m_path;
  FinishedCB m_finishedCB;
};
}

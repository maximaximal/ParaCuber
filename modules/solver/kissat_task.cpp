#include "kissat_task.hpp"
#include "paracooba/common/status.h"
#include "paracooba/common/task.h"

#include <cassert>

#include <paracooba/common/log.h>
#include <paracooba/module.h>

extern "C" {
#include <kissat/application.h>
#include <kissat/kissat.h>
}

namespace parac::solver {
struct KissatTask::Internal {
  Internal(parac_handle& handle)
    : handle(handle)
    , solver(kissat_init(), kissat_release) {}

  parac_handle& handle;

  std::unique_ptr<kissat, void (*)(kissat*)> solver;
};

KissatTask::KissatTask(parac_handle& handle, const char* file, parac_task& task)
  : m_internal(std::make_unique<Internal>(handle))
  , m_file(file)
  , m_task(task) {
  task.state = PARAC_TASK_WORK_AVAILABLE;
  task.result = PARAC_PENDING;
  task.userdata = this;
  task.terminate = static_terminate;
  task.work = static_work;
  task.free_userdata = static_free_userdata;
}

KissatTask::~KissatTask() {}

parac_status
KissatTask::static_work(parac_task* task, parac_worker worker) {
  assert(task);
  assert(task->userdata);
  KissatTask* self = static_cast<KissatTask*>(task->userdata);
  return self->work(worker);
}
void
KissatTask::static_terminate(parac_task* task) {
  assert(task);
  assert(task->userdata);
  KissatTask* self = static_cast<KissatTask*>(task->userdata);
  return self->terminate();
}
parac_status
KissatTask::static_free_userdata(parac_task* task) {
  assert(task);
  if(task->userdata) {
    KissatTask* self = static_cast<KissatTask*>(task->userdata);
    delete self;
    task->userdata = nullptr;
  }
  return PARAC_OK;
}

parac_status
KissatTask::work(parac_worker worker) {
  parac_log(PARAC_SOLVER,
            PARAC_DEBUG,
            "Starting Kissat parser and solver on worker {} for formula in {}",
            worker,
            m_file);
  const int argc = 2;
  const char* argv[argc + 1]{ "kissat", m_file, nullptr };
  int result = kissat_application(
    m_internal->solver.get(), argc, const_cast<char**>(argv));
  switch(result) {
    case 0:
      parac_log(PARAC_SOLVER, PARAC_DEBUG, "Kissat was aborted!", m_file);
      return PARAC_ABORTED;
    case 10:
      parac_log(PARAC_SOLVER,
                PARAC_DEBUG,
                "Kissat found the formula {} to be SAT!",
                m_file);
      return PARAC_SAT;
    case 20:
      parac_log(PARAC_SOLVER,
                PARAC_DEBUG,
                "Kissat found the formula {} to be UNSAT!",
                m_file);
      return PARAC_UNSAT;
    default:
      return PARAC_UNKNOWN;
  }
}
void
KissatTask::terminate() {
  kissat_terminate(m_internal->solver.get());
}

}

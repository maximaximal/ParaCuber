#include <algorithm>
#include <list>
#include <vector>

#include "cadical_handle.hpp"
#include "cadical_manager.hpp"
#include "solver_task.hpp"

#include <paracooba/common/log.h>
#include <paracooba/module.h>
#include <paracooba/runner/runner.h>

#include <boost/pool/pool_alloc.hpp>

namespace parac::solver {
struct CaDiCaLManager::Internal {
  template<typename T>
  using Allocator = boost::fast_pool_allocator<
    T,
    boost::default_user_allocator_new_delete,
    boost::details::pool::null_mutex,// No mutex required, as accesses are
                                     // synchronized by containerMutex
    64,
    128>;
  struct SolverTaskWrapper;
  using SolverTaskWrapperList =
    std::list<SolverTaskWrapper, Allocator<SolverTaskWrapper>>;
  struct SolverTaskWrapper {
    SolverTaskWrapperList::iterator it;
    SolverTask t;
  };

  static SolverTaskWrapperList solverTasks;
  static std::mutex solverTasksMutex;

  std::vector<CaDiCaLHandlePtr> solvers;
  std::vector<bool> borrowed;// Default constructor of bool is false.
};

CaDiCaLManager::Internal::SolverTaskWrapperList
  CaDiCaLManager::Internal::solverTasks;
std::mutex CaDiCaLManager::Internal::solverTasksMutex;

CaDiCaLManager::CaDiCaLManager(parac_module& mod,
                               CaDiCaLHandlePtr parsedFormula)
  : m_internal(std::make_unique<Internal>())
  , m_mod(mod)
  , m_parsedFormula(std::move(parsedFormula)) {
  uint32_t workers = 0;

  if(mod.handle->modules[PARAC_MOD_RUNNER]) {
    workers =
      mod.handle->modules[PARAC_MOD_RUNNER]->runner->available_worker_count;
  }

  parac_log(
    PARAC_SOLVER,
    PARAC_TRACE,
    "Generate CaDiCaLManager for formula in file \"{}\" from compute node {} "
    "for {} "
    "workers. Copy operation is deferred to when a solver is requested.",
    m_parsedFormula->path(),
    m_parsedFormula->originatorId(),
    workers);

  m_internal->solvers.resize(workers);
  m_internal->borrowed.resize(workers);
}
CaDiCaLManager::~CaDiCaLManager() {
  parac_log(PARAC_SOLVER,
            PARAC_TRACE,
            "Destroy CaDiCaLManager for formula in file \"{}\" from {}.",
            m_parsedFormula->path(),
            m_parsedFormula->originatorId());
}

CaDiCaLManager::CaDiCaLHandlePtr
CaDiCaLManager::takeHandleForWorker(parac_worker worker) {
  auto& s = m_internal->solvers;
  auto& b = m_internal->borrowed;

  assert(worker < s.size());
  assert(!b[worker]);

  b[worker] = true;

  if(!s[worker]) {
    parac_log(PARAC_SOLVER,
              PARAC_DEBUG,
              "Creating copy of root formula in file \"{}\" from {} because a "
              "solver object was requested from CaDiCaLManager.",
              m_parsedFormula->path(),
              m_parsedFormula->originatorId());
    s[worker] = std::make_unique<CaDiCaLHandle>(*m_parsedFormula);
  }
  return std::move(s[worker]);
}
void
CaDiCaLManager::returnHandleFromWorker(CaDiCaLHandlePtr handle,
                                       parac_worker worker) {
  auto& s = m_internal->solvers;
  auto& b = m_internal->borrowed;

  assert(worker < s.size());
  assert(b[worker]);

  b[worker] = false;

  s[worker] = std::move(handle);
}

SolverTask*
CaDiCaLManager::createSolverTask(parac_task& task) {
  std::unique_lock lock(Internal::solverTasksMutex);
  auto& wrapper = Internal::solverTasks.emplace_front();
  wrapper.t.init(*this, task);
  wrapper.it = Internal::solverTasks.begin();
  return &wrapper.t;
}
void
CaDiCaLManager::deleteSolverTask(SolverTask* task) {
  assert(task);
  Internal::SolverTaskWrapper* taskWrapper =
    reinterpret_cast<Internal::SolverTaskWrapper*>(
      reinterpret_cast<std::byte*>(task) -
      offsetof(Internal::SolverTaskWrapper, t));
  assert(taskWrapper);
  std::unique_lock lock(Internal::solverTasksMutex);
  Internal::solverTasks.erase(taskWrapper->it);
}
}

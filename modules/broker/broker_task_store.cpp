#include <algorithm>
#include <cassert>
#include <deque>
#include <list>
#include <map>
#include <mutex>
#include <set>
#include <unordered_map>

#include "broker_compute_node_store.hpp"
#include "broker_task_store.hpp"

#include "paracooba/broker/broker.h"
#include "paracooba/common/compute_node.h"
#include "paracooba/common/compute_node_store.h"
#include "paracooba/common/log.h"
#include "paracooba/common/path.h"
#include "paracooba/common/status.h"
#include "paracooba/common/task.h"
#include "paracooba/common/task_store.h"
#include "paracooba/common/types.h"
#include "paracooba/module.h"

#include <boost/pool/pool_alloc.hpp>

namespace parac::broker {
static ComputeNodeStore&
extractComputeNodeStore(parac_handle& h) {
  assert(h.modules[PARAC_MOD_BROKER]);
  parac_module_broker& broker = *h.modules[PARAC_MOD_BROKER]->broker;
  assert(broker.compute_node_store);
  assert(broker.compute_node_store->userdata);
  return *static_cast<ComputeNodeStore*>(broker.compute_node_store->userdata);
}

static void
incrementWorkQueueInComputeNode(parac_handle& h, parac_id originator) {
  ComputeNodeStore& brokerComputeNodeStore = extractComputeNodeStore(h);
  brokerComputeNodeStore.incrementThisNodeWorkQueueSize(originator);
}

static void
decrementWorkQueueInComputeNode(parac_handle& h, parac_id originator) {
  ComputeNodeStore& brokerComputeNodeStore = extractComputeNodeStore(h);
  brokerComputeNodeStore.decrementThisNodeWorkQueueSize(originator);
}

struct TaskStore::Internal {
  explicit Internal(parac_handle& handle, parac_task_store& store)
    : handle(handle)
    , store(store) {}

  std::recursive_mutex assessMutex;
  std::mutex containerMutex;
  parac_handle& handle;
  parac_task_store& store;

  struct Task;
  template<typename T>
  using Allocator = boost::fast_pool_allocator<
    T,
    boost::default_user_allocator_new_delete,
    boost::details::pool::null_mutex,// No mutex required, as accesses are
                                     // synchronized by containerMutex
    64,
    128>;
  using TaskList = std::list<Task, Allocator<Task>>;

  struct Task {
    TaskList::iterator it;
    parac_task t;
    short refcount = 0;

    ~Task() {
      parac_log(PARAC_BROKER,
                PARAC_TRACE,
                "Delete task on path {} with address {}",
                t.path,
                static_cast<void*>(&t));
      if(t.free_userdata) {
        t.free_userdata(&t);
      }
    }
  };
  using TaskRef = std::reference_wrapper<parac_task>;

  struct TaskRefCompare {
    bool operator()(const TaskRef& lhs, const TaskRef& rhs) const {
      return static_cast<parac::Path&>(lhs.get().path) >
             static_cast<parac::Path&>(rhs.get().path);
    }
  };

  TaskList tasks;

  std::list<TaskRef, Allocator<TaskRef>> tasksWaitingForWorkerQueue;

  std::set<TaskRef, TaskRefCompare, Allocator<TaskRef>> tasksBeingWorkedOn;
  std::set<TaskRef, TaskRefCompare, Allocator<TaskRef>> tasksWaitingForChildren;

  std::unordered_map<parac_compute_node*,
                     std::set<TaskRef, TaskRefCompare, Allocator<TaskRef>>>
    offloadedTasks;
};

TaskStore::TaskStore(parac_handle& handle, parac_task_store& s)
  : m_internal(std::make_unique<Internal>(handle, s)) {

  s.userdata = this;
  s.empty = [](parac_task_store* store) {
    assert(store);
    assert(store->userdata);
    return static_cast<TaskStore*>(store->userdata)->empty();
  };
  s.get_size = [](parac_task_store* store) {
    assert(store);
    assert(store->userdata);
    return static_cast<TaskStore*>(store->userdata)->size();
  };
  s.get_waiting_for_worker_size = [](parac_task_store* store) {
    assert(store);
    assert(store->userdata);
    return static_cast<TaskStore*>(store->userdata)
      ->m_internal->tasksWaitingForWorkerQueue.size();
  };
  s.get_waiting_for_children_size = [](parac_task_store* store) {
    assert(store);
    assert(store->userdata);
    return static_cast<TaskStore*>(store->userdata)
      ->m_internal->tasksWaitingForChildren.size();
  };
  s.get_tasks_being_worked_on_size = [](parac_task_store* store) {
    assert(store);
    assert(store->userdata);
    return static_cast<TaskStore*>(store->userdata)
      ->m_internal->tasksBeingWorkedOn.size();
  };
  s.new_task = [](parac_task_store* store,
                  parac_task* creator_task,
                  parac_path new_path,
                  parac_id originator) {
    assert(store);
    assert(store->userdata);
    return static_cast<TaskStore*>(store->userdata)
      ->newTask(creator_task, new_path, originator);
  };
  s.pop_offload = [](parac_task_store* store, parac_compute_node* target) {
    assert(store);
    assert(store->userdata);
    assert(target);
    return static_cast<TaskStore*>(store->userdata)->pop_offload(target);
  };
  s.pop_work = [](parac_task_store* store) {
    assert(store);
    assert(store->userdata);
    return static_cast<TaskStore*>(store->userdata)->pop_work();
  };
  s.assess_task = [](parac_task_store* store, parac_task* task) {
    assert(store);
    assert(store->userdata);
    assert(task);
    return static_cast<TaskStore*>(store->userdata)->assess_task(task);
  };
  s.ping_on_work_userdata = nullptr;
  s.ping_on_work = nullptr;
}
TaskStore::~TaskStore() {
  parac_log(PARAC_BROKER, PARAC_DEBUG, "Destroy TaskStore.");
}

bool
TaskStore::empty() const {
  return m_internal->tasks.empty();
}
size_t
TaskStore::size() const {
  return m_internal->tasks.size();
}
parac_task*
TaskStore::newTask(parac_task* parent_task,
                   parac_path new_path,
                   parac_id originator) {
  std::unique_lock lock(m_internal->containerMutex);
  auto& taskWrapper = m_internal->tasks.emplace_front();
  parac_task* task = &taskWrapper.t;
  taskWrapper.it = m_internal->tasks.begin();
  parac_task_init(task);
  task->parent_task_ = parent_task;
  task->task_store = &m_internal->store;
  task->path = new_path;
  task->originator = originator;

  if(parent_task) {
    Internal::Task* parentWrapper = reinterpret_cast<Internal::Task*>(
      reinterpret_cast<std::byte*>(parent_task) - offsetof(Internal::Task, t));
    ++(parentWrapper->refcount);

    if(parac_path_get_next_left(parent_task->path).rep == new_path.rep) {
      parent_task->left_child_ = task;
    } else if(parac_path_get_next_right(parent_task->path).rep ==
              new_path.rep) {
      parent_task->right_child_ = task;
    }
  }

  return task;
}

parac_task*
TaskStore::pop_offload(parac_compute_node* target) {
  std::unique_lock lock(m_internal->containerMutex);
  if(m_internal->tasksWaitingForWorkerQueue.empty()) {
    return nullptr;
  }
  parac_task& t = m_internal->tasksWaitingForWorkerQueue.back();
  m_internal->tasksWaitingForWorkerQueue.pop_back();
  m_internal->offloadedTasks[target].insert(t);
  t.state = static_cast<parac_task_state>(t.state & ~PARAC_TASK_WORK_AVAILABLE);
  t.state = t.state | PARAC_TASK_OFFLOADED;

  Internal::Task* taskWrapper = reinterpret_cast<Internal::Task*>(
    reinterpret_cast<std::byte*>(&t) - offsetof(Internal::Task, t));
  ++taskWrapper->refcount;

  decrementWorkQueueInComputeNode(m_internal->handle, t.originator);

  return &t;
}
parac_task*
TaskStore::pop_work() {
  std::unique_lock lock(m_internal->containerMutex);
  if(m_internal->tasksWaitingForWorkerQueue.empty()) {
    return nullptr;
  }
  parac_task& t = m_internal->tasksWaitingForWorkerQueue.front();
  m_internal->tasksWaitingForWorkerQueue.pop_front();
  m_internal->tasksBeingWorkedOn.insert(t);
  t.state = static_cast<parac_task_state>(t.state & ~PARAC_TASK_WORK_AVAILABLE);
  t.state = t.state | PARAC_TASK_WORKING;

  Internal::Task* taskWrapper = reinterpret_cast<Internal::Task*>(
    reinterpret_cast<std::byte*>(&t) - offsetof(Internal::Task, t));
  ++taskWrapper->refcount;

  decrementWorkQueueInComputeNode(m_internal->handle, t.originator);

  return &t;
}

void
TaskStore::insert_into_tasksWaitingForWorkerQueue(parac_task* task) {
  assert(task);

  task->state = task->state | PARAC_TASK_WORK_AVAILABLE;

  {
    std::unique_lock lock(m_internal->containerMutex);
    m_internal->tasksWaitingForWorkerQueue.insert(
      std::lower_bound(m_internal->tasksWaitingForWorkerQueue.begin(),
                       m_internal->tasksWaitingForWorkerQueue.end(),
                       task->path,
                       [](const Internal::TaskRef& t, parac_path p) {
                         return t.get().path.length > p.length;
                       }),
      *task);

    incrementWorkQueueInComputeNode(m_internal->handle, task->originator);
  }

  if(m_internal->store.ping_on_work) {
    m_internal->store.ping_on_work(&m_internal->store);
  }
}

void
TaskStore::insert_into_tasksWaitingForChildren(parac_task* task) {
  assert(task);
  std::unique_lock lock(m_internal->containerMutex);
  m_internal->tasksWaitingForChildren.insert(*task);
}
void
TaskStore::remove_from_tasksWaitingForChildren(parac_task* task) {
  assert(task);
  std::unique_lock lock(m_internal->containerMutex);
  m_internal->tasksWaitingForChildren.erase(*task);
}

void
TaskStore::insert_into_tasksBeingWorkedOn(parac_task* task) {
  assert(task);
  std::unique_lock lock(m_internal->containerMutex);
  m_internal->tasksBeingWorkedOn.insert(*task);
}
void
TaskStore::remove_from_tasksBeingWorkedOn(parac_task* task) {
  assert(task);
  std::unique_lock lock(m_internal->containerMutex);
  m_internal->tasksBeingWorkedOn.erase(*task);
}

void
TaskStore::remove(parac_task* task) {
  Internal::Task* taskWrapper = reinterpret_cast<Internal::Task*>(
    reinterpret_cast<std::byte*>(task) - offsetof(Internal::Task, t));
  assert(taskWrapper);

  std::unique_lock lock(m_internal->containerMutex);

  --taskWrapper->refcount;

  if(taskWrapper->refcount > 0)
    return;

  assert(task);
  assert(!(task->state & PARAC_TASK_WAITING_FOR_SPLITS));
  assert(!(task->state & PARAC_TASK_WORK_AVAILABLE));
  assert(task->state & PARAC_TASK_DONE);

  // Remove references to now removed path.
  if(task->left_child_)
    task->left_child_->parent_task_ = nullptr;
  if(task->right_child_)
    task->right_child_->parent_task_ = nullptr;

  if(task->parent_task_) {
    Internal::Task* parentWrapper = reinterpret_cast<Internal::Task*>(
      reinterpret_cast<std::byte*>(task->parent_task_) -
      offsetof(Internal::Task, t));
    --parentWrapper->refcount;

    if(task->parent_task_->left_child_ == task)
      task->parent_task_->left_child_ = nullptr;
    if(task->parent_task_->right_child_ == task)
      task->parent_task_->right_child_ = nullptr;
  }

  m_internal->tasks.erase(taskWrapper->it);
}

void
TaskStore::assess_task(parac_task* task) {
  assert(task);
  assert(task->assess);
  assert(task->path.rep != PARAC_PATH_EXPLICITLY_UNKNOWN);

  // Required so that no concurrent assessments of the same tasks are happening.
  std::unique_lock lock(m_internal->assessMutex);

  parac_task_state s = task->assess(task);
  task->state = s;

  parac_log(PARAC_BROKER,
            PARAC_TRACE,
            "Assessing task on path {} to state {}. Work available in store: "
            "{}, tasks in store: {}",
            task->path,
            task->state,
            m_internal->tasksWaitingForWorkerQueue.size(),
            m_internal->tasks.size());

  if(parac_task_state_is_done(s)) {
    assert(!(s & PARAC_TASK_WORK_AVAILABLE));

    remove_from_tasksBeingWorkedOn(task);

    if(m_internal->handle.input_file && parac_path_is_root(task->path)) {
      m_internal->handle.request_exit(&m_internal->handle);
      m_internal->handle.exit_status = task->result;
    }

    if((s & PARAC_TASK_SPLITS_DONE) == PARAC_TASK_SPLITS_DONE) {
      remove_from_tasksWaitingForChildren(task);
    }

    auto path = task->path;
    auto result = task->result;
    parac_task* parent = task->parent_task_;

    remove(task);

    parac_log(PARAC_BROKER,
              PARAC_TRACE,
              "Task on path {} done with result {}",
              path,
              result);

    if(parent) {
      parac_path parentPath = parent->path;

      if(path.rep == parac_path_get_next_left(parentPath).rep) {
        parent->left_result = result;
      } else if(path.rep == parac_path_get_next_right(parentPath).rep) {
        parent->right_result = result;
      } else {
        parac_log(PARAC_BROKER,
                  PARAC_GLOBALERROR,
                  "Task on path {} specified parent path on path {}, but is no "
                  "direct child of parent! Ignoring parent.",
                  path,
                  parentPath);
        return;
      }

      assess_task(parent);
    }

    return;
  }

  // Must be at the end, because priority inversion could finish the task and
  // immediately remove it, leading to double invocations and chaos!
  if(s & PARAC_TASK_WORK_AVAILABLE && !(s & PARAC_TASK_DONE)) {
    insert_into_tasksWaitingForWorkerQueue(task);
  }
  if(!(task->last_state & PARAC_TASK_WAITING_FOR_SPLITS) &&
     s & PARAC_TASK_WAITING_FOR_SPLITS) {
    insert_into_tasksWaitingForChildren(task);
  }

  task->last_state = s;
}
}

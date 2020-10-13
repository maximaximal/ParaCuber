#include "paracooba/common/compute_node_store.h"
#include "paracooba/common/config.h"
#include "paracooba/common/types.h"
#include <paracooba/broker/broker.h>
#include <paracooba/module.h>

#include "paracooba/common/log.h"

#include <cassert>

#include <parac_broker_export.h>

#include "broker_compute_node_store.hpp"

#define BROKER_NAME "cpp_nodemap_keepstocked"
#define BROKER_VERSION_MAJOR 1
#define BROKER_VERSION_MINOR 0
#define BROKER_VERSION_PATCH 0
#define BROKER_VERSION_TWEAK 0

struct BrokerUserdata {
  BrokerUserdata(parac_module_broker& mod_broker)
    : store(store_interface) {
    mod_broker.compute_node_store = &store_interface;
  }

  parac_compute_node_store store_interface;
  parac::broker::ComputeNodeStore store;
};

static parac_status
pre_init(parac_module* mod) {
  assert(mod);
  assert(mod->broker);
  assert(mod->handle);
  assert(mod->handle->config);

  BrokerUserdata* userdata = new BrokerUserdata(*mod->broker);
  mod->userdata = userdata;

  return PARAC_OK;
}

static parac_status
init(parac_module* mod) {
  assert(mod);
  assert(mod->broker);
  assert(mod->handle);
  assert(mod->handle->config);
  assert(mod->handle->thread_registry);
  assert(mod->userdata);

  return PARAC_OK;
}

static parac_status
mod_request_exit(parac_module* mod) {
  assert(mod);
  assert(mod->broker);
  assert(mod->handle);

  return PARAC_OK;
}

static parac_status
mod_exit(parac_module* mod) {
  assert(mod);
  assert(mod->broker);
  assert(mod->handle);
  assert(mod->userdata);

  BrokerUserdata *userdata = static_cast<BrokerUserdata*>(mod->userdata);
  delete userdata;

  return PARAC_OK;
}

extern "C" PARAC_BROKER_EXPORT parac_status
parac_module_discover_broker(parac_handle* handle) {
  assert(handle);

  parac_module* mod = handle->prepare(handle, PARAC_MOD_BROKER);
  assert(mod);

  mod->type = PARAC_MOD_BROKER;
  mod->name = BROKER_NAME;
  mod->version.major = BROKER_VERSION_MAJOR;
  mod->version.minor = BROKER_VERSION_MINOR;
  mod->version.patch = BROKER_VERSION_PATCH;
  mod->version.tweak = BROKER_VERSION_TWEAK;
  mod->pre_init = pre_init;
  mod->init = init;
  mod->request_exit = mod_request_exit;
  mod->exit = mod_exit;

  return PARAC_OK;
}
#include "../include/paracooba/util.hpp"
#include "../include/paracooba/client.hpp"
#include "../include/paracooba/config.hpp"
#include "../include/paracooba/daemon.hpp"
#include <cmath>

namespace paracooba {
std::string
BytePrettyPrint(size_t bytes)
{
  auto base = (double)std::log(bytes) / (double)std::log(1024);
  const char* suffixArr[] = { "B", "kiB", "MiB", "GiB", "TiB", "PiB" };
  return std::to_string(
           (size_t)std::round(std::pow(1024, base - std::floor(base)))) +
         suffixArr[(size_t)std::floor(base)];
}

std::shared_ptr<CNF>
GetRootCNF(Config* config, int64_t cnfID)
{
  assert(config);
  if(config->isDaemonMode()) {
    Daemon* daemon = config->getDaemon();
    auto [context, lock] = daemon->getContext(cnfID);
    if(context) {
      return context->getRootCNF();
    } else {
      return nullptr;
    }
  } else {
    Client* client = config->getClient();
    return client->getRootCNF();
  }
}
}

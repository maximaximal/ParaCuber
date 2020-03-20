#include <boost/program_options.hpp>
#include <iostream>

#include <paracooba/client.hpp>
#include <paracooba/cnf.hpp>
#include <paracooba/communicator.hpp>
#include <paracooba/config.hpp>
#include <paracooba/daemon.hpp>
#include <paracooba/log.hpp>
#include <paracooba/util.hpp>

namespace po = boost::program_options;
using namespace paracooba;

int
main(int argc, char* argv[])
{
  // Start of main.
  std::shared_ptr<Config> config = std::make_shared<Config>();
  bool continueRunning = config->parseParameters(argc, argv);
  if(!continueRunning) {
    return EXIT_SUCCESS;
  }

  std::shared_ptr<Log> log = std::make_shared<Log>(config);
  auto logger = log->createLogger("main");

  PARACOOBA_LOG(logger, Trace)
    << "Starting paracooba \"" << config->getString(Config::LocalName)
    << "\" in " << (config->isDaemonMode() ? "daemon mode" : "client mode");

  CommunicatorPtr communicator = std::make_shared<Communicator>(config, log);

  communicator->startRunner();

  std::unique_ptr<Client> client;
  std::unique_ptr<Daemon> daemon;

  if(config->isDaemonMode()) {
    daemon = std::make_unique<Daemon>(config, log, communicator);
  } else {
    client = std::make_unique<Client>(config, log, communicator);
    client->solve();
  }

  try {
    communicator->run();
  } catch(std::exception& e) {
    PARACOOBA_LOG(logger, LocalError)
      << "Encountered exception which was not catched until main()! Message: "
      << e.what();
  }

  if(!config->isDaemonMode()) {
    // Client mode. There should be some action.
    TaskResult::Status status = client->getStatus();

    try {
      std::string_view dumpTree = config->getString(Config::DumpTreeAtExit);
      if(dumpTree != "") {
        PARACOOBA_LOG(logger, Trace) << "Dump CNF Tree to file " << dumpTree;
        client->getRootCNF()->getCNFTree().dumpTreeToFile(dumpTree);
      }
    } catch(const std::exception& e) {
      PARACOOBA_LOG(logger, LocalError)
        << "Dump CNF Tree to file failed! Error: " << e.what();
    }

    switch(status) {
      case TaskResult::Unsolved:
        std::cout << "unknown" << std::endl;
        break;
      case TaskResult::Satisfiable:
        std::cout << "sat" << std::endl;
        break;
      case TaskResult::Unsatisfiable:
        std::cout << "unsat" << std::endl;
        break;
      default:
        std::cout << "invalid result" << std::endl;
        break;
    }
  } else {
  }

  PARACOOBA_LOG(logger, Trace) << "Ending paracooba.";
  return EXIT_SUCCESS;
}

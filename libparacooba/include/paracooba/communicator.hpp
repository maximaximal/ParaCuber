#ifndef PARACOOBA_COMMUNICATOR_HPP
#define PARACOOBA_COMMUNICATOR_HPP

#include "cluster-statistics.hpp"
#include "cnftree.hpp"
#include "log.hpp"
#include "messages/jobdescription_receiver.hpp"
#include "readywaiter.hpp"
#include "webserver/initiator.hpp"

#include <functional>
#include <memory>

#include <boost/asio/high_resolution_timer.hpp>
#include <boost/asio/io_service.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/version.hpp>

namespace boost {
namespace system {
class error_code;
}
}

namespace paracooba {

namespace messages {
class CNFTreeNodeStatusRequest;
class JobDescriptionReceiverProvider;
}

namespace net {
class UDPServer;
class TCPAcceptor;
class Control;
}

class Runner;
class CNF;
class NetworkedNode;

using RunnerPtr = std::shared_ptr<Runner>;
using ClusterStatisticsPtr = std::shared_ptr<ClusterStatistics>;

/** @brief Hub for all local & network communication processes between nodes.
 *
 * This node owns the boost asio io_service that is responsible for all timing
 * operations on the local node and for communicating with other nodes.
 *
 * \section SendAndReceiveFormulas Sending and Receiving Formulas
 *
 * A solver is not sent directly. Instead, only the changes to the formula state
 * are transmitted. This means, that the root formula is sent once and cubes are
 * sent as needed. More on synchronising cubes in @ref CNFTree.
 *
 * \dotfile solver-network-flow.dot
 */
class Communicator : public std::enable_shared_from_this<Communicator>
{
  public:
  /** @brief Constructor */
  Communicator(ConfigPtr config, LogPtr log);
  /** @brief Destructor */
  virtual ~Communicator();

  /** @brief Runs the communicator thread and blocks until termination.
   *
   * This starts the communicator worker thread. It stops after all work is
   * completed.
   */
  void run();

  /** @brief Ends the communicator and stops all running services.
   */
  void exit();

  /** @brief Start the internal runner thread pool without blocking. */
  void startRunner();

  /** @brief Get the active \ref Runner class instance for running \ref Task
   * objects.
   */
  inline RunnerPtr getRunner() { return m_runner; }
  inline boost::asio::io_service& getIOService() { return m_ioService; }
  inline ClusterStatisticsPtr getClusterStatistics()
  {
    return m_clusterStatistics;
  }

  void injectCNFTreeNodeInfo(int64_t cnfId,
                             int64_t handle,
                             Path p,
                             CNFTree::State state,
                             int64_t remote);

  void requestCNFTreePathInfo(
    const messages::CNFTreeNodeStatusRequest& request);

  private:
  friend class webserver::API;

  ConfigPtr m_config;
  LogPtr m_log;
  boost::asio::io_service m_ioService;
  boost::asio::io_service::work m_ioServiceWork;

  Logger m_logger;
  std::unique_ptr<boost::asio::signal_set> m_signalSet;
  RunnerPtr m_runner;

  std::unique_ptr<net::Control> m_control;

  std::unique_ptr<net::UDPServer> m_udpServer;
  std::unique_ptr<net::TCPAcceptor> m_tcpAcceptor;

  ClusterStatisticsPtr m_clusterStatistics;

  int64_t m_currentMessageId = INT64_MIN;

  void signalHandler(const boost::system::error_code& error, int signalNumber);

  void checkAndTransmitClusterStatisticsChanges(bool force = false);

  messages::JobDescriptionReceiverProvider& getJobDescriptionReceiverProvider();

  // Listeners
  bool listenForIncomingUDP(uint16_t port);
  bool listenForIncomingTCP(uint16_t port);

  /** @brief Ticks are called every 100ms.
   *
   * The main task of ticks is to send out statistics updates to all other
   * connected nodes.
   */
  void tick();
  boost::asio::high_resolution_timer m_tickTimer;

  std::unique_ptr<webserver::Initiator> m_webserverInitiator;
};

using CommunicatorPtr = std::shared_ptr<Communicator>;
}

#endif

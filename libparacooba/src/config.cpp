#include "../include/paracooba/config.hpp"
#include "../include/paracooba/cluster-statistics.hpp"
#include "../include/paracooba/communicator.hpp"
#include "../include/paracooba/messages/node.hpp"
#include "../include/paracooba/runner.hpp"
#include <boost/asio/ip/host_name.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/exception/diagnostic_information.hpp>
#include <boost/filesystem.hpp>
#include <boost/program_options.hpp>
#include <cstddef>
#include <iostream>
#include <random>
#include <thread>

namespace po = boost::program_options;
namespace fs = boost::filesystem;

namespace paracooba {
Config::Config()
  : m_optionsCLI("CLI-only options")
  , m_optionsCommon("CLI and config file options")
  , m_optionsFile("Config file options")
{
  /* CLI ONLY OPTIONS
   * --------------------------------------- */
  // clang-format off
  m_optionsCLI.add_options()
    ("help", "produce help message")
    ;
  // clang-format on

  {
    m_generatedLocalName =
      boost::asio::ip::host_name() + ".local;" + std::to_string(getpid());
    set(LocalName, m_generatedLocalName);
  }

  /* CONFIG FILES ONLY OPTIONS
   * --------------------------------------- */

  std::random_device dev;
  std::mt19937_64 rng(dev());
  std::uniform_int_distribution<std::mt19937_64::result_type> dist_mac(
    -((int64_t)1 << 47), ((int64_t)1 << 47) - 1);

  /* COMMON OPTIONS
   * --------------------------------------- */
  // clang-format off
  uint32_t threadCount = std::thread::hardware_concurrency();

  m_optionsCommon.add_options()
    (GetConfigNameFromEnum(Config::LocalName),
         po::value<std::string>()->default_value(m_generatedLocalName)->value_name("string"), "local name of this solver node")
    (GetConfigNameFromEnum(Config::InputFile),
         po::value<std::string>()->default_value("")->value_name("string"), "input file (problem) to parse")
    (GetConfigNameFromEnum(Config::ThreadCount),
         po::value<uint32_t>()->default_value(threadCount)->value_name("int"),
         "number of worker threads to execute tasks on")
    (GetConfigNameFromEnum(Config::UDPListenPort),
         po::value<uint16_t>()->default_value(18001)->value_name("int"),
         "udp port for incoming control messages")
    (GetConfigNameFromEnum(Config::UDPTargetPort),
         po::value<uint16_t>()->default_value(18001)->value_name("int"),
         "udp port for outgoing control messages")
    (GetConfigNameFromEnum(Config::TCPListenPort),
         po::value<uint16_t>()->default_value(18001)->value_name("int"),
         "tcp port for incoming data messages")
    (GetConfigNameFromEnum(Config::TCPTargetPort),
         po::value<uint16_t>()->default_value(18001)->value_name("int"),
         "tcp port for outgoing data messages")
    (GetConfigNameFromEnum(Config::HTTPListenPort),
         po::value<uint16_t>()->default_value(18080)->value_name("int"),
         "port for internal webserver")
    (GetConfigNameFromEnum(Config::ConnectionRetries),
         po::value<uint16_t>()->default_value(5)->value_name("int"),
         "number of times connections are tried to be re-established after they were lost")
    (GetConfigNameFromEnum(Config::HTTPDocRoot),
         po::value<std::string>()->default_value(getInternalWebserverDefaultDocRoot())->value_name("string"),
         "document root path of the internal http server")
    (GetConfigNameFromEnum(Config::IPAddress),
     po::value<std::string>()->default_value(boost::asio::ip::address_v4().to_string())->value_name("string"),
         "IP address to use for network communication")
    (GetConfigNameFromEnum(Config::IPBroadcastAddress),
     po::value<std::string>()->default_value(boost::asio::ip::address_v4().broadcast().to_string())->value_name("string"),
         "IP broadcast address to use for network communication")
    (GetConfigNameFromEnum(Config::DumpTreeAtExit),
     po::value<std::string>()->default_value("")->value_name("string"),
         "Dump binary tree to file at exit. Empty string disables dump.")
    (GetConfigNameFromEnum(Config::AutoShutdown),
     po::value<int32_t>()->default_value(-1)->value_name("int"),
         "automatically shutdown n seconds after no tasks are running anymore. -1 means disabled.")
    (GetConfigNameFromEnum(Config::Id),
         po::value<int64_t>()->default_value(dist_mac(rng))->value_name("int"),
         "Unique Number (only 48 Bit) (can be MAC address)")
    (GetConfigNameFromEnum(Config::WorkQueueCapacity),
         po::value<uint64_t>()->default_value(threadCount)->value_name("int"),
         "Capacity of the internal work queue. Should be high on daemons and low on clients.")
    (GetConfigNameFromEnum(Config::TickMilliseconds),
         po::value<uint64_t>()->default_value(500)->value_name("int"),
         "Milliseconds between Communicator ticks.")
    (GetConfigNameFromEnum(Config::FreqCuberCutoff),
         po::value<float>()->default_value(0.1)->value_name("float"),
         "Cutoff point (ratio of assigned literals to unassigned ones)) at which the literal frequency cuber algorithm stops creating new cubes.")
    (GetConfigNameFromEnum(Config::MaxNodeUtilization),
         po::value<float>()->default_value(2)->value_name("float"),
         "Maximum node utilization to still offload a decision to there.")
    (GetConfigNameFromEnum(Config::DaemonHost),
     po::value<std::string>()->default_value("127.0.0.1")->value_name("string"),
         "Initial peer to connect to. Should should be a long-running daemon.")
    ("debug,d", po::bool_switch(&m_debugMode)->default_value(false)->value_name("bool"), "debug mode (all debug output)")
    ("network-debug,nd", po::bool_switch(&m_networkDebugMode)->default_value(false)->value_name("bool"), "network debug mode (all debug output, including network communication)")
    ("info,i", po::bool_switch(&m_infoMode)->default_value(false)->value_name("bool"), "info mode (more information)")
    ("daemon", po::bool_switch(&m_daemonMode)->default_value(false)->value_name("bool"), "daemon mode")
    ("enable-client-cadical", po::bool_switch(&m_enableClientCaDiCaL)->default_value(false)->value_name("bool"), "direct solving via CaDiCaL on client")
    ("enable-internal-webserver", po::bool_switch(&m_enableInternalWebserver)->default_value(false)->value_name("bool"), "enable internal webserver to see debugging, diagnostics and status information")
    (GetConfigNameFromEnum(Config::LogToSTDOUT), po::bool_switch(&m_useSTDOUTForLogging)->default_value(false)->value_name("bool"), "use stdout for logging")
    (GetConfigNameFromEnum(Config::LimitedTreeDump), po::bool_switch(&m_limitedTreeDump)->default_value(true)->value_name("bool"), "Limit tree dumping to only contain unvisited and unknown nodes, do not further explore known results.")
    ;
  // clang-format on
}

Config::~Config() {}

int64_t
Config::generateId(int64_t uniqueNumber)
{
  int16_t pid = static_cast<int16_t>(::getpid());
  return ((int64_t)pid << 48) | uniqueNumber;
}

ID
Config::getId() const
{
  return getInt64(Config::Id);
}

bool
Config::parseParameters(int argc, char** argv)
{
  static char* argv_default[] = { (char*)"", nullptr };
  if(argc == 0 && argv == nullptr) {
    argc = 1;
    argv = argv_default;
  }

  po::positional_options_description positionalOptions;
  positionalOptions.add("input-file", 1);

  po::options_description cliGroup;
  cliGroup.add(m_optionsCommon).add(m_optionsCLI);
  po::variables_map vm;
  try {
    po::store(po::command_line_parser(argc, argv)
                .options(cliGroup)
                .positional(positionalOptions)
                .run(),
              vm);
    po::notify(vm);
  } catch(const std::exception& e) {
    std::cout << "Could not parse CLI Parameters! Error: " << e.what()
              << std::endl;
    return false;
  }

  if(vm.count("help")) {
    std::cout << m_optionsCLI << std::endl;
    std::cout << m_optionsCommon << std::endl;
    std::cout << m_optionsFile << std::endl;
    return false;
  }
  return processCommonParameters(vm);
}

bool
Config::parseConfigFile(std::string_view filePath)
{
  return true;
}

std::string
Config::getKeyAsString(Key key)
{
  ConfigVariant& v = get(key);
  switch(v.index()) {
    case 0:
      return std::to_string(std::get<uint16_t>(v));
    case 1:
      return std::to_string(std::get<uint32_t>(v));
    case 2:
      return std::to_string(std::get<uint64_t>(v));
    case 3:
      return std::to_string(std::get<int32_t>(v));
    case 4:
      return std::to_string(std::get<int64_t>(v));
    case 5:
      return std::to_string(std::get<float>(v));
    case 6:
      return std::get<std::string>(v);
    default:
      return "Unknown Type!";
  }
}

template<typename T>
inline void
conditionallySetConfigOptionToArray(
  const boost::program_options::variables_map& vm,
  Config::ConfigVariant* arr,
  Config::Key key)
{
  if(vm.count(GetConfigNameFromEnum(key))) {
    arr[key] = vm[GetConfigNameFromEnum(key)].as<T>();
  }
}

bool
Config::processCommonParameters(const boost::program_options::variables_map& vm)
{
  conditionallySetConfigOptionToArray<std::string>(
    vm, m_config.data(), Config::LocalName);
  conditionallySetConfigOptionToArray<std::string>(
    vm, m_config.data(), Config::InputFile);
  conditionallySetConfigOptionToArray<uint32_t>(
    vm, m_config.data(), Config::ThreadCount);
  conditionallySetConfigOptionToArray<uint16_t>(
    vm, m_config.data(), Config::UDPListenPort);
  conditionallySetConfigOptionToArray<uint16_t>(
    vm, m_config.data(), Config::UDPTargetPort);
  conditionallySetConfigOptionToArray<uint16_t>(
    vm, m_config.data(), Config::TCPListenPort);
  conditionallySetConfigOptionToArray<uint16_t>(
    vm, m_config.data(), Config::TCPTargetPort);
  conditionallySetConfigOptionToArray<uint16_t>(
    vm, m_config.data(), Config::HTTPListenPort);
  conditionallySetConfigOptionToArray<uint16_t>(
    vm, m_config.data(), Config::ConnectionRetries);
  conditionallySetConfigOptionToArray<std::string>(
    vm, m_config.data(), Config::HTTPDocRoot);
  conditionallySetConfigOptionToArray<int32_t>(
    vm, m_config.data(), Config::AutoShutdown);
  conditionallySetConfigOptionToArray<uint64_t>(
    vm, m_config.data(), Config::WorkQueueCapacity);
  conditionallySetConfigOptionToArray<uint64_t>(
    vm, m_config.data(), Config::TickMilliseconds);
  conditionallySetConfigOptionToArray<float>(
    vm, m_config.data(), Config::FreqCuberCutoff);
  conditionallySetConfigOptionToArray<float>(
    vm, m_config.data(), Config::MaxNodeUtilization);
  conditionallySetConfigOptionToArray<std::string>(
    vm, m_config.data(), Config::DaemonHost);
  conditionallySetConfigOptionToArray<std::string>(
    vm, m_config.data(), Config::IPAddress);
  conditionallySetConfigOptionToArray<std::string>(
    vm, m_config.data(), Config::IPBroadcastAddress);
  conditionallySetConfigOptionToArray<std::string>(
    vm, m_config.data(), Config::DumpTreeAtExit);

  if(vm.count(GetConfigNameFromEnum(Id))) {
    m_config[Id] = generateId(vm[GetConfigNameFromEnum(Id)].as<int64_t>());
  }

  return true;
}

#define CHECK_PATH(PATH) \
  if(fs::exists(PATH))   \
    return fs::absolute(PATH).string();

std::string
Config::getInternalWebserverDefaultDocRoot()
{
  CHECK_PATH("../internalwebserver-docroot/")
  CHECK_PATH("/usr/local/share/paracooba/internalwebserver/")
  CHECK_PATH("/usr/share/paracooba/internalwebserver/")
  return "/usr/local/share/paracooba/internalwebserver/";
}

messages::Node
Config::buildNode() const
{
  uint64_t maxCPUFreq = 0;
  uint64_t workQueueSize = getCommunicator()->getRunner()->getWorkQueueSize();
  uint32_t uptime = 0;

  messages::Node node(std::string(getString(Key::LocalName)),
                      getInt64(Key::Id),
                      getUint32(Key::ThreadCount),
                      getUint64(Key::WorkQueueCapacity),
                      workQueueSize,
                      maxCPUFreq,
                      uptime,
                      getUint16(Key::UDPListenPort),
                      getUint16(Key::TCPListenPort),
                      isDaemonMode());

  // Populate known hosts from cluster statistics, if available.
  if(m_communicator && m_communicator->getClusterStatistics()) {
    auto clusterStatistics = m_communicator->getClusterStatistics();
    auto [map, lock] = clusterStatistics->getNodeMap();
    for(auto& it : map) {
      auto& clusterNode = it.second;
      NetworkedNode* nn = clusterNode.getNetworkedNode();
      if(nn) {
        node.addKnownPeerFromNetworkedNode(nn);
      }
    }
  }

  return std::move(node);
}
}

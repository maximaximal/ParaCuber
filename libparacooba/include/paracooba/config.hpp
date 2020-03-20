#ifndef PARACOOBA_CONFIG_HPP

#include <array>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/variables_map.hpp>
#include <cstddef>
#include <string_view>
#include <variant>

namespace paracooba {
class Client;
class Daemon;
class Communicator;

namespace webserver {
class API;
}

namespace messages {
class Node;
}

/** @brief Store for communication options specific for each node.
 *
 */
class Config
{
  public:
  /** Configuration variable differentiator enumeration.
   */
  enum Key
  {
    LocalName,
    InputFile,
    ThreadCount,
    UDPTargetPort,
    UDPListenPort,
    TCPTargetPort,
    TCPListenPort,
    IPAddress,
    IPBroadcastAddress,
    Id,
    DaemonHost,
    WorkQueueCapacity,
    MaxNodeUtilization,
    TickMilliseconds,
    HTTPListenPort,
    HTTPDocRoot,
    LogToSTDOUT,
    AutoShutdown,
    DumpTreeAtExit,
    LimitedTreeDump,
    ConnectionRetries,

    FreqCuberCutoff,

    _KEY_COUNT
  };
  using ConfigVariant = std::
    variant<uint16_t, uint32_t, uint64_t, int32_t, int64_t, float, std::string>;

  /** @brief Constructor
   */
  Config();
  /** @brief Destructor.
   */
  ~Config();

  /** @brief Parse command line parameters and also process provided
   * configuration files.
   *
   * @return True if program execution may continue, false if program should be
   * terminated.
   */
  bool parseParameters(int argc = 0, char* argv[] = nullptr);
  /** @brief Parse command line parameters and also process provided
   * configuration files.
   *
   * @return True if program execution may continue, false if program should be
   * terminated.
   */
  bool parseConfigFile(std::string_view filePath);

  std::string getKeyAsString(Key key);

  /** @brief Get a configuration variable with type and key.
   */
  template<typename T>
  inline T& get(Key key)
  {
    return std::get<T>(m_config[key]);
  }
  /** @brief Get a std::string configuration variable.
   */
  inline std::string_view getString(Key key)
  {
    std::string& str = get<std::string>(key);
    return std::string_view{ str.c_str(), str.size() };
  }
  /** @brief Get an uint16 configuration variable.
   */
  inline uint16_t getUint16(Key key) { return get<uint16_t>(key); }
  /** @brief Get an uint32 configuration variable.
   */
  inline uint32_t getUint32(Key key) { return get<uint32_t>(key); }
  /** @brief Get an uint64 configuration variable.
   */
  inline uint64_t getUint64(Key key) { return get<uint64_t>(key); }
  /** @brief Get an int32 configuration variable.
   */
  inline int32_t getInt32(Key key) { return get<int32_t>(key); }
  /** @brief Get an int64 configuration variable.
   */
  inline int64_t getInt64(Key key) { return get<int64_t>(key); }
  /** @brief Get a float configuration variable.
   */
  inline float getFloat(Key key) { return get<float>(key); }

  /** @brief Get a configuration variable which can be cast in any way.
   */
  inline ConfigVariant& get(Key key) { return m_config[key]; }
  /** @brief Set a configuration variable.
   */
  inline void set(Key key, ConfigVariant&& val) { m_config[key] = val; }

  /** @brief Get a configuration variable which can be cast in any way.
   */
  ConfigVariant& operator[](Key key) { return get(key); }

  /** @brief Check if debug mode is active. */
  inline bool isDebugMode() { return m_debugMode; }
  /** @brief Check if network debug mode is active. */
  inline bool isNetworkDebugMode() { return m_networkDebugMode; }
  /** @brief Check if info mode is active. */
  inline bool isInfoMode() { return m_infoMode; }
  /** @brief Check if daemon mode is active. */
  inline bool isDaemonMode() { return m_daemonMode; }

  /** @brief Set debug mode active. */
  inline void setDebugMode(bool v) { m_debugMode = v; }
  /** @brief Set network debug mode active. */
  inline void setNetworkDebugMode(bool v) { m_networkDebugMode = v; }
  /** @brief Set info mode active. */
  inline void setInfoMode(bool v) { m_infoMode = v; }
  /** @brief Set daemon mode active. */
  inline void setDaemonMode(bool v) { m_daemonMode = v; }

  /** @brief Check if STDOUT should be used for logging instead of CLOG. */
  inline bool useSTDOUTForLogging() { return m_useSTDOUTForLogging; }
  /** @brief Check if direct client-side solving via CaDiCaL is enabled. */
  inline bool isClientCaDiCaLEnabled() { return m_enableClientCaDiCaL; }
  /** @brief Check if internal webserver is enabled. */
  inline bool isInternalWebserverEnabled() { return m_enableInternalWebserver; }
  /** @brief Check if limited tree dump is set. */
  inline bool isLimitedTreeDumpActive() { return m_limitedTreeDump; }

  int64_t generateId(int64_t uniqueNumber);

  Client* getClient()
  {
    assert(m_client);
    return m_client;
  }
  bool hasClient() { return m_daemon; }
  Daemon* getDaemon()
  {
    assert(m_daemon);
    return m_daemon;
  }
  bool hasDaemon() { return m_daemon; }
  Communicator* getCommunicator()
  {
    assert(m_communicator);
    return m_communicator;
  }
  bool hasCommunicator() { return m_communicator; }

  messages::Node buildNode();

  private:
  friend class Client;
  friend class Daemon;
  friend class Communicator;

  bool processCommonParameters(
    const boost::program_options::variables_map& map);

  using ConfigArray =
    std::array<ConfigVariant, static_cast<std::size_t>(_KEY_COUNT)>;
  ConfigArray m_config;

  boost::program_options::options_description m_optionsCLI;
  boost::program_options::options_description m_optionsCommon;
  boost::program_options::options_description m_optionsFile;

  bool m_debugMode = false;
  bool m_networkDebugMode = false;
  bool m_infoMode = false;
  bool m_daemonMode = false;
  bool m_enableClientCaDiCaL = false;
  bool m_useSTDOUTForLogging = false;
  bool m_limitedTreeDump = true;
#ifdef ENABLE_INTERNAL_WEBSERVER
  bool m_enableInternalWebserver = true;
#else
  bool m_enableInternalWebserver = false;
#endif

  std::string m_generatedLocalName;

  std::string getInternalWebserverDefaultDocRoot();

  Communicator* m_communicator = nullptr;
  Client* m_client = nullptr;
  Daemon* m_daemon = nullptr;
};

constexpr const char*
GetConfigNameFromEnum(Config::Key key)
{
  switch(key) {
    case Config::LocalName:
      return "local-name";
    case Config::InputFile:
      return "input-file";
    case Config::ThreadCount:
      return "threads";
    case Config::UDPListenPort:
      return "udp-listen-port";
    case Config::UDPTargetPort:
      return "udp-target-port";
    case Config::TCPListenPort:
      return "tcp-listen-port";
    case Config::TCPTargetPort:
      return "tcp-target-port";
    case Config::HTTPListenPort:
      return "http-listen-port";
    case Config::HTTPDocRoot:
      return "http-doc-root";
    case Config::Id:
      return "id";
    case Config::DaemonHost:
      return "daemon-host";
    case Config::WorkQueueCapacity:
      return "work-queue-capacity";
    case Config::FreqCuberCutoff:
      return "frequency-cuber-cutoff";
    case Config::TickMilliseconds:
      return "tick-milliseconds";
    case Config::MaxNodeUtilization:
      return "max-node-utilization";
    case Config::LogToSTDOUT:
      return "log-to-stdout";
    case Config::IPAddress:
      return "ip-address";
    case Config::IPBroadcastAddress:
      return "ip-broadcast-address";
    case Config::AutoShutdown:
      return "auto-shutdown";
    case Config::DumpTreeAtExit:
      return "dump-tree-at-exit";
    case Config::LimitedTreeDump:
      return "limited-tree-dump";
    case Config::ConnectionRetries:
      return "connection-retries";
    default:
      return "";
  }
}
}

#endif

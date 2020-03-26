#ifndef PARACOOBA_LOG_HPP
#define PARACOOBA_LOG_HPP

#include <boost/log/attributes/constant.hpp>
#include <boost/log/attributes/mutable_constant.hpp>
#include <boost/log/sinks/sync_frontend.hpp>
#include <boost/log/sinks/text_ostream_backend.hpp>
#include <boost/log/sources/record_ostream.hpp>
#include <boost/log/sources/severity_feature.hpp>
#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/utility/formatting_ostream.hpp>
#include <boost/log/utility/manipulators/to_log.hpp>
#include <memory>
#include <string>
#include <thread>

#include "util.hpp"

#ifndef FILE_BASENAME
#define FILE_BASENAME "Basename not supported."
#endif

template<typename T>
using MutableConstant = boost::log::attributes::mutable_constant<T>;

extern thread_local MutableConstant<int> lineAttr;
extern thread_local MutableConstant<const char*> fileAttr;
extern thread_local MutableConstant<const char*> functionAttr;
extern thread_local MutableConstant<std::string_view> threadNameAttr;
extern thread_local MutableConstant<std::string_view> localNameAttr;

namespace paracooba {
class Config;
class Log;
using ConfigPtr = std::shared_ptr<Config>;
using LogPtr = std::shared_ptr<Log>;

/** @brief Utility class to manage logging.
 */
class Log
{
  public:
  /** @brief Tag to associate severity internally.
   */
  struct Severity_Tag;
  /** @brief Severity of a log message.
   *
   * Local errors should be differentiated from global errors! Local errors only
   * affect local node, a global error may affect the entire distributed solving
   * process over multiple nodes.
   */
  enum Severity
  {
    NetTrace,
    Trace,
    Debug,
    Info,
    LocalWarning,
    LocalError,
    GlobalWarning,
    GlobalError,
    Fatal
  };

  template<typename LoggerType>
  struct Handle
  {
    Handle(LoggerType logger, Log& log)
      : logger(logger)
      , log(log)
    {}
    Handle(const Handle& o)
      : logger(o.logger)
      , log(o.log)
    {}
    Handle& operator=(const Handle& o)
    {
      logger = o.logger;
      log = o.log;
      return *this;
    }
    LoggerType logger;
    Log& log;
  };

  using Logger = Handle<boost::log::sources::severity_logger<Log::Severity>>;
  using LoggerMT =
    Handle<boost::log::sources::severity_logger_mt<Log::Severity>>;

  /** @brief Constructor
   */
  Log(ConfigPtr config);
  /** @brief Destructor.
   */
  ~Log();

  /** @brief Create a logger for a specific environment, which may receive
   * multiple custom attributes.
   */
  Logger createLogger(const std::string& context, const std::string& meta = "");
  LoggerMT createLoggerMT(const std::string& context,
                          const std::string& meta = "");

  bool isLogLevelEnabled(Severity severity);

  struct ThreadLocalData
  {
    explicit ThreadLocalData(const std::string& threadName)
      : threadName(threadName)
    {}

    std::string threadName;
  };
  std::map<std::thread::id, ThreadLocalData> threadLocalData;
  ThreadLocalData& getThreadLocalData()
  {
    auto it = threadLocalData.find(std::this_thread::get_id());
    assert(it != threadLocalData.end());
    return it->second;
  }
  void initLocalThread(const std::string& threadName)
  {
    auto id = std::this_thread::get_id();
    assert(!threadLocalData.count(id));
    threadLocalData.insert(std::make_pair(id, ThreadLocalData(threadName)));
  }
  std::string_view getLocalName();

  private:
  ConfigPtr m_config;
  boost::shared_ptr<boost::log::sinks::synchronous_sink<
    boost::log::sinks::basic_text_ostream_backend<char>>>
    m_consoleSink;
  Severity m_targetSeverity = LocalWarning;
};

using Logger = Log::Logger;
using LoggerMT = Log::LoggerMT;

std::ostream&
operator<<(std::ostream& strm, ::paracooba::Log::Severity level);

boost::log::formatting_ostream&
operator<<(
  boost::log::formatting_ostream& strm,
  boost::log::to_log_manip<::paracooba::Log::Severity,
                           ::paracooba::Log::Severity_Tag> const& manip);
}

#define PARACOOBA_LOG_LOCATION() \
  lineAttr.set(__LINE__);        \
  fileAttr.set(FILE_BASENAME);   \
  functionAttr.set(__PRETTY_FUNCTION__);

#define PARACOOBA_LOG(LOGGER, SEVERITY)                               \
  do {                                                                \
    PARACOOBA_LOG_LOCATION()                                          \
    localNameAttr.set((LOGGER).log.getLocalName());                   \
    threadNameAttr.set((LOGGER).log.getThreadLocalData().threadName); \
  } while(false);                                                     \
  BOOST_LOG_SEV((LOGGER).logger, ::paracooba::Log::Severity::SEVERITY)

#endif

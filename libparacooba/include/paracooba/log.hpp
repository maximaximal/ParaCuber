#ifndef PARACOOBA_LOG_HPP
#define PARACOOBA_LOG_HPP

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

#include "util.hpp"

#ifndef FILE_BASENAME
#define FILE_BASENAME "Basename not supported."
#endif

template<typename T>
using MutableConstant = boost::log::attributes::mutable_constant<T>;

extern thread_local MutableConstant<int> lineAttr;
extern thread_local MutableConstant<const char*> fileAttr;
extern thread_local MutableConstant<const char*> functionAttr;
extern thread_local MutableConstant<std::string> threadNameAttr;
extern thread_local std::string paracCurrentThreadName;

#define PARACOOBA_LOG_LOCATION(lg) \
  lineAttr.set(__LINE__);          \
  fileAttr.set(FILE_BASENAME);     \
  functionAttr.set(__PRETTY_FUNCTION__);

#define PARACOOBA_LOG(LOGGER, SEVERITY)         \
  do {                                          \
    PARACOOBA_LOG_LOCATION(LOGGER)              \
    threadNameAttr.set(paracCurrentThreadName); \
  } while(false);                               \
  BOOST_LOG_SEV(LOGGER, ::paracooba::Log::Severity::SEVERITY)

namespace paracooba {
class Config;
using ConfigPtr = std::shared_ptr<Config>;

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

  /** @brief Constructor
   */
  Log(ConfigPtr config);
  /** @brief Destructor.
   */
  ~Log();

  /** @brief Create a logger for a specific environment, which may receive
   * multiple custom attributes.
   */
  boost::log::sources::severity_logger<Log::Severity> createLogger(
    const std::string& context,
    const std::string& meta = "");
  boost::log::sources::severity_logger_mt<Log::Severity> createLoggerMT(
    const std::string& context,
    const std::string& meta = "");

  bool isLogLevelEnabled(Severity severity);

  private:
  ConfigPtr m_config;
  boost::shared_ptr<boost::log::sinks::synchronous_sink<
    boost::log::sinks::basic_text_ostream_backend<char>>>
    m_consoleSink;
  Severity m_targetSeverity = LocalWarning;
};

using LogPtr = std::shared_ptr<Log>;
using Logger = boost::log::sources::severity_logger<Log::Severity>;
using LoggerPtr =
  std::shared_ptr<boost::log::sources::severity_logger<Log::Severity>>;
using LoggerMT = boost::log::sources::severity_logger_mt<Log::Severity>;

std::ostream&
operator<<(std::ostream& strm, ::paracooba::Log::Severity level);

boost::log::formatting_ostream&
operator<<(
  boost::log::formatting_ostream& strm,
  boost::log::to_log_manip<::paracooba::Log::Severity,
                           ::paracooba::Log::Severity_Tag> const& manip);
}

#endif

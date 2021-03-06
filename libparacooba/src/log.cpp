#include "../include/paracooba/log.hpp"
#include "../include/paracooba/config.hpp"
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/log/attributes.hpp>
#include <boost/log/attributes/constant.hpp>
#include <boost/log/core.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/expressions/predicates/has_attr.hpp>
#include <boost/log/sinks/text_file_backend.hpp>
#include <boost/log/sources/global_logger_storage.hpp>
#include <boost/log/sources/logger.hpp>
#include <boost/log/sources/record_ostream.hpp>
#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/support/date_time.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>
#include <boost/log/utility/setup/console.hpp>
#include <boost/log/utility/setup/file.hpp>
#include <boost/phoenix/bind.hpp>

namespace logging = boost::log;
namespace sinks = boost::log::sinks;
namespace expr = boost::log::expressions;
namespace keywords = boost::log::keywords;

BOOST_LOG_ATTRIBUTE_KEYWORD(paracooba_logger_severity,
                            "Severity",
                            ::paracooba::Log::Severity)
BOOST_LOG_ATTRIBUTE_KEYWORD(paracooba_logger_file, "File", std::string)
BOOST_LOG_ATTRIBUTE_KEYWORD(paracooba_logger_function, "Function", std::string)
BOOST_LOG_ATTRIBUTE_KEYWORD(paracooba_logger_line, "Line", int)
BOOST_LOG_ATTRIBUTE_KEYWORD(paracooba_logger_localname,
                            "LocalName",
                            std::string)
BOOST_LOG_ATTRIBUTE_KEYWORD(paracooba_logger_timestamp,
                            "Timestamp",
                            boost::posix_time::ptime)

BOOST_LOG_ATTRIBUTE_KEYWORD(paracooba_logger_context, "Context", std::string)
BOOST_LOG_ATTRIBUTE_KEYWORD(paracooba_logger_context_meta,
                            "ContextMeta",
                            std::string)

BOOST_LOG_ATTRIBUTE_KEYWORD(paracooba_logger_thread_name,
                            "ThreadName",
                            std::string)

thread_local MutableConstant<int> lineAttr = MutableConstant<int>(0);
thread_local MutableConstant<const char*> fileAttr =
  MutableConstant<const char*>("");
thread_local MutableConstant<const char*> functionAttr =
  MutableConstant<const char*>("");
thread_local MutableConstant<std::string> threadNameAttr =
  MutableConstant<std::string>("");
thread_local MutableConstant<std::string> localNameAttr =
  MutableConstant<std::string>("Unnamed Thread");

bool LogSinksSetup = false;

namespace paracooba {

BOOST_LOG_INLINE_GLOBAL_LOGGER_INIT(global_logger,
                                    ::boost::log::sources::logger)
{
  logging::sources::logger lg;
  lg.add_attribute("Line", lineAttr);
  lg.add_attribute("File", fileAttr);
  lg.add_attribute("Function", functionAttr);
  lg.add_attribute("ThreadName", threadNameAttr);
  return lg;
}

Log::Log(ConfigPtr config)
  : m_config(config)
{
  initLocalThread("Main");
  assert(getThreadLocalData().threadName == "Main");

  // Initialise global logging attributes for all loggers.
  try {
    logging::core::get()->add_global_attribute(
      "Timestamp", logging::attributes::local_clock());
    logging::core::get()->add_global_attribute("LocalName", localNameAttr);
    logging::core::get()->add_global_attribute("ThreadName", threadNameAttr);
    logging::add_common_attributes();

    m_severityConfig[static_cast<size_t>(NetTrace)] =
      config->isNetworkTraceMode();
    m_severityConfig[static_cast<size_t>(Cubes)] = config->isTraceMode();
    m_severityConfig[static_cast<size_t>(Trace)] = config->isTraceMode();
    m_severityConfig[static_cast<size_t>(NetDebug)] =
      config->isNetworkDebugMode();
    m_severityConfig[static_cast<size_t>(Debug)] = config->isDebugMode();
    m_severityConfig[static_cast<size_t>(Info)] =
      config->isInfoMode() || config->isDebugMode();
    m_severityConfig[static_cast<size_t>(LocalWarning)] = true;
    m_severityConfig[static_cast<size_t>(LocalError)] = true;
    m_severityConfig[static_cast<size_t>(GlobalWarning)] = true;
    m_severityConfig[static_cast<size_t>(GlobalError)] = true;
    m_severityConfig[static_cast<size_t>(Fatal)] = true;
  } catch(const std::exception& e) {
    std::cerr
      << "> Exception during initialisation of global log variables! Error: "
      << e.what() << ". Log level config: " << debugSeverityConfig()
      << std::endl;
    BOOST_THROW_EXCEPTION(e);
  }
  try {
    auto& targetStream =
      m_config->useSTDOUTForLogging() ? std::cout : std::clog;

    if(!LogSinksSetup) {
      m_consoleSink = logging::add_console_log(
        targetStream,
        keywords::format =
          (expr::stream
           << "c [" << paracooba_logger_timestamp << "] ["
           << paracooba_logger_localname << "] ["
           << expr::attr<Log::Severity, Log::Severity_Tag>("Severity") << "] ["
           << paracooba_logger_thread_name << "] "
           << expr::if_(expr::has_attr<std::string>(
                "ContextMeta"))[expr::stream
                                << "[" << paracooba_logger_context << "<"
                                << paracooba_logger_context_meta << ">]"]
                .else_[expr::stream << "[" << paracooba_logger_context << "]"]
           << " " << expr::smessage));
      boost::log::core::get()->add_sink(m_consoleSink);
      LogSinksSetup = true;
    }
  } catch(const std::exception& e) {
    std::cerr << "> Exception during initialisation of log sinks! Error: "
              << e.what() << std::endl;
    BOOST_THROW_EXCEPTION(e);
  }
}
Log::~Log() {}

template<typename LoggerType>
static LoggerType
createGenericLogger(const std::string& context)
{
  auto lg = LoggerType();
  auto contextConstant = boost::log::attributes::make_constant(context);
  lg.add_attribute("Context", contextConstant);
  return std::move(lg);
}

template<typename Logger>
static Log::Handle<Logger>
createGenericLoggerHandle(Logger&& logger, Log& log, const std::string& meta)
{
  auto handle = Log::Handle<Logger>{ logger, log };
  if(meta != "") {
    handle.setMeta(meta);
  }
  return std::move(handle);
}

Logger
Log::createLogger(const std::string& context, const std::string& meta)
{
  return createGenericLoggerHandle(
    createGenericLogger<boost::log::sources::severity_logger<Log::Severity>>(
      context),
    *this,
    meta);
}
LoggerMT
Log::createLoggerMT(const std::string& context, const std::string& meta)
{
  return createGenericLoggerHandle(
    createGenericLogger<boost::log::sources::severity_logger_mt<Log::Severity>>(
      context),
    *this,
    meta);
}

std::string_view
Log::getLocalName() const
{
  return m_config->getString(Config::LocalName);
}

static const char* strings[] = { "NetTrace",      "Trace",        "Debug",
                                 "Info",          "LocalWarning", "LocalError",
                                 "GlobalWarning", "GlobalError",  "Fatal" };
static const char* colorised_strings[] = {
  "\033[0;37mNTRC\033[0m", "\033[0;37mCUBE\033[0m", "\033[0;37mTRCE\033[0m",
  "\033[0;37mNDBG\033[0m", "\033[0;32mDEBG\033[0m", "\033[1;37mINFO\033[0m",
  "\033[0;33mLWRN\033[0m", "\033[0;33mLERR\033[0m", "\033[0;31mGWRN\033[0m",
  "\033[0;31mGERR\033[0m", "\033[0;35mFTAL\033[0m",
};
static const char* uncolorised_strings[] = { "NTRC", "CUBE", "TRCE", "NDBG",
                                             "DEBG", "INFO", "LWRN", "LERR",
                                             "GWRN", "GERR", "FTAL" };

#define SEV_PRINT_HELPER(Severity)

std::string
Log::debugSeverityConfig() const
{
  std::string out("Enabled Log Levels:");
  for(size_t severity = 0; severity <= static_cast<size_t>(Fatal); ++severity) {
    out += " ";
    out += (m_severityConfig[severity] ? uncolorised_strings[severity] : "");
  }
  return out;
}

std::ostream&
operator<<(std::ostream& strm, ::paracooba::Log::Severity level)
{
  if(static_cast<std::size_t>(level) < sizeof(strings) / sizeof(*strings))
    strm << strings[level];
  else
    strm << static_cast<int>(level);

  return strm;
}

boost::log::formatting_ostream&
operator<<(
  boost::log::formatting_ostream& strm,
  boost::log::to_log_manip<::paracooba::Log::Severity,
                           ::paracooba::Log::Severity_Tag> const& manip)
{
  static const char** strings = uncolorised_strings;

  const char* terminal = std::getenv("TERM");

  if(terminal == NULL) {
    strings = uncolorised_strings;
  } else {
    if(std::strlen(terminal) > 7) {
      strings = colorised_strings;
    } else {
      strings = uncolorised_strings;
    }
  }

  ::paracooba::Log::Severity level = manip.get();

  if(static_cast<std::size_t>(level) <
     sizeof(uncolorised_strings) / sizeof(*strings))
    strm << strings[level];
  else
    strm << static_cast<int>(level);

  return strm;
}
}

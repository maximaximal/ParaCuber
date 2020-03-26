#ifndef PARACOOBA_WEBSERVER_API_HPP
#define PARACOOBA_WEBSERVER_API_HPP

#define BOOST_SPIRIT_THREADSAFE
#include "../cnftree.hpp"
#include "../log.hpp"
#include "webserver.hpp"
#include <boost/asio/ip/tcp.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/version.hpp>
#include <memory>
#include <regex>

namespace paracooba {
class ClusterStatistics;
namespace webserver {

class API
{
  public:
  enum Request
  {
    LocalConfig,
    LocalInfo,

    Unknown,
  };

  using WebSocketCB =
    std::add_pointer<bool(std::weak_ptr<Webserver::HTTPSession>&,
                          boost::property_tree::ptree&)>::type;
  using WebSocketCBData =
    std::add_pointer<bool(std::weak_ptr<Webserver::HTTPSession>&,
                          const std::string&)>::type;

  explicit API(Webserver* webserver, ConfigPtr config, LogPtr log);
  ~API();

  void injectCNFTreeNode(int64_t handle,
                         Path p,
                         CNFTree::State state,
                         int64_t remote);

  void injectClusterStatisticsUpdate(ClusterStatistics& stats);

  static bool isAPIRequest(const std::string& target);
  static Request matchTargetToRequest(const std::string& target);

  std::string generateResponse(Request request);

  std::string generateResponseForLocalConfig();
  std::string generateResponseForLocalInfo();

  void handleWebSocketRequest(const boost::asio::ip::tcp::socket* socket,
                              std::weak_ptr<Webserver::HTTPSession> session,
                              WebSocketCB cb,
                              WebSocketCBData,
                              boost::property_tree::ptree* ptree);

  void handleWebSocketClosed(const boost::asio::ip::tcp::socket* socket);

  struct WSData
  {
    explicit WSData(std::weak_ptr<Webserver::HTTPSession> session,
                    WebSocketCB cb,
                    WebSocketCBData dataCB,
                    int64_t cnfId)
      : session(session)
      , cb(cb)
      , dataCB(dataCB)
      , answer()
      , cnfId(cnfId)
    {}
    std::weak_ptr<Webserver::HTTPSession> session;
    WebSocketCB cb;
    WebSocketCBData dataCB;
    boost::property_tree::ptree answer;
    int64_t cnfId;
  };
  using WSPair =
    std::pair<const boost::asio::ip::tcp::socket*, std::unique_ptr<WSData>>;
  using WSMap =
    std::map<const boost::asio::ip::tcp::socket*, std::unique_ptr<WSData>>;
  WSMap m_wsData;

  bool handleInjectedCNFTreeNode(WSData& d,
                                 Path p,
                                 CNFTree::State state,
                                 int64_t remote);

  bool sendError(WSData& d, const std::string& str);

  static const std::regex matchAPIRequest;
  static const std::regex matchLocalConfigRequest;
  static const std::regex matchLocalInfoRequest;

  private:
  Webserver* m_webserver;
  ConfigPtr m_config;
  Logger m_logger;

  void conditionalEraseConn(const boost::asio::ip::tcp::socket* socket,
                            bool erase);
};
}
}

#endif

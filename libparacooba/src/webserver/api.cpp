#include "../../include/paracooba/webserver/api.hpp"
#include "../../include/paracooba/client.hpp"
#include "../../include/paracooba/cluster-statistics.hpp"
#include "../../include/paracooba/cnf.hpp"
#include "../../include/paracooba/communicator.hpp"
#include "../../include/paracooba/config.hpp"
#include "../../include/paracooba/cuber/registry.hpp"
#include "../../include/paracooba/webserver/webserver.hpp"

#include "../../include/paracooba/messages/cnftree_node_status_request.hpp"
#include <cassert>
#include <regex>
#include <sstream>

#include <sys/resource.h>

namespace paracooba {
namespace webserver {
const std::regex API::matchAPIRequest("^/api/.*");
const std::regex API::matchLocalConfigRequest("^/api/local-config.json$");
const std::regex API::matchLocalInfoRequest("^/api/local-info.json$");

API::API(Webserver* webserver, ConfigPtr config, LogPtr log)
  : m_webserver(webserver)
  , m_logger(log->createLogger("API"))
  , m_config(config)
{}
API::~API() {}

void
API::injectCNFTreeNode(int64_t handle,
                       Path p,
                       CNFTree::State state,
                       int64_t remote)
{
  auto socket = reinterpret_cast<const boost::asio::ip::tcp::socket*>(handle);
  auto it = m_wsData.find(socket);
  if(it == m_wsData.end()) {
    PARACOOBA_LOG(m_logger, LocalWarning)
      << "Invalid socket handle supplied to API in CNFTreeNode injection!";
    return;
  }
  WSData& data = *it->second;

  conditionalEraseConn(socket,
                       handleInjectedCNFTreeNode(data, p, state, remote));
}
void
API::injectClusterStatisticsUpdate(ClusterStatistics& stats)
{
  // Cluster Statistics can be directly converted into a JSON data stream, so it
  // can be sent directly.
  std::stringstream sstream;
  sstream << stats;
  std::string data = sstream.str();

  for(auto& it : m_wsData) {
    auto& conn = *it.second;
    conditionalEraseConn(it.first, conn.dataCB(conn.session, data));
  }
}

bool
API::isAPIRequest(const std::string& target)
{
  return std::regex_match(target, matchAPIRequest);
}

API::Request
API::matchTargetToRequest(const std::string& target)
{
  if(std::regex_match(target, matchLocalInfoRequest))
    return LocalInfo;
  if(std::regex_match(target, matchLocalConfigRequest))
    return LocalConfig;
  return Unknown;
}

std::string
API::generateResponse(Request request)
{
  assert(m_config);
  assert(m_webserver);
  switch(request) {
    case LocalConfig:
      return generateResponseForLocalConfig();
    case LocalInfo:
      return generateResponseForLocalInfo();
    default:
      return "{\"success\":false}";
  }
}

std::string
API::generateResponseForLocalConfig()
{
  std::string str = "{\n";
  for(std::size_t i = 0; i < Config::_KEY_COUNT; ++i) {
    auto key = static_cast<Config::Key>(i);
    str += "  \"" + std::string(GetConfigNameFromEnum(key)) + "\":\"" +
           m_config->getKeyAsString(key) + "\"";
    if(i < Config::_KEY_COUNT - 1)
      str += ",";
    str += "\n";
  }
  str += "}";
  return str;
}
std::string
API::generateResponseForLocalInfo()
{
  std::string str = "{\n";
  if(!m_config->isDaemonMode()) {
    str += "  \"CNFVarCount\":" +
           std::to_string(m_config->getClient()->getCNFVarCount()) + "\n";
  }
  str += "}";
  return str;
}

void
API::handleWebSocketRequest(const boost::asio::ip::tcp::socket* socket,
                            std::weak_ptr<Webserver::HTTPSession> session,
                            WebSocketCB cb,
                            WebSocketCBData dataCB,
                            boost::property_tree::ptree* ptree)
{
  if(!ptree) {
    m_wsData.erase(socket);
    return;
  }
  auto it = m_wsData.find(socket);
  if(it == m_wsData.end()) {
    std::shared_ptr<CNF> cnf;

    int64_t cnfId = 0;
    if(!m_config->isDaemonMode()) {
      cnf = m_config->getClient()->getRootCNF();
      cnfId = cnf->getOriginId();
    }

    it = m_wsData
           .insert(WSPair(socket,
                          std::make_unique<WSData>(session, cb, dataCB, cnfId)))
           .first;

    if(m_config->isDaemonMode()) {
      // Daemons only show cluster stats.
    } else {
      // First, wait for the allowance map to be ready.
      cnf->rootTaskReady.callWhenReady([this, cnf, socket](CaDiCaLTask& ptr) {
        // Registry is only initialised after the root task arrived.
        cnf->getCuberRegistry().allowanceMapWaiter.callWhenReady(
          [this, cnf, socket](cuber::Registry::AllowanceMap& map) {
            m_config->getCommunicator()->requestCNFTreePathInfo(
              messages::CNFTreeNodeStatusRequest(
                reinterpret_cast<int64_t>(socket),
                CNFTree::buildPath(0, 0),
                cnf->getOriginId()));
            m_config->getCommunicator()
              ->checkAndTransmitClusterStatisticsChanges(true);
          });
      });
    }
  }
  assert(it->second);
  WSData& data = *it->second;

  std::string type = ptree->get<std::string>("type");

  if(type == "cnftree-request-path") {
    // Request path.
    std::string strPath = ptree->get<std::string>("path");
    bool next = ptree->get<bool>("next");

    if(strPath.length() > CNFTree::maxPathDepth) {
      return conditionalEraseConn(socket,
                                  sendError(data, "CNFTree path too long!"));
    }

    Path p = CNFTree::strToPath(strPath.data(), strPath.length());
    m_config->getCommunicator()->requestCNFTreePathInfo(
      messages::CNFTreeNodeStatusRequest(
        reinterpret_cast<int64_t>(socket), p, data.cnfId));
    if(next) {
      m_config->getCommunicator()->requestCNFTreePathInfo(
        messages::CNFTreeNodeStatusRequest(reinterpret_cast<int64_t>(socket),
                                           CNFTree::getNextLeftPath(p),
                                           data.cnfId));

      m_config->getCommunicator()->requestCNFTreePathInfo(
        messages::CNFTreeNodeStatusRequest(reinterpret_cast<int64_t>(socket),
                                           CNFTree::getNextRightPath(p),
                                           data.cnfId));
    } else {
      m_config->getCommunicator()->requestCNFTreePathInfo(
        messages::CNFTreeNodeStatusRequest(
          reinterpret_cast<int64_t>(socket), p, data.cnfId));
    }

  } else if(type == "ping") {
    // Ping
    data.answer.put("type", "pong");
    conditionalEraseConn(socket, cb(data.session, data.answer));
  } else {
    return conditionalEraseConn(
      socket, sendError(data, "Unknown message type \"" + type + "\"!"));
  }
}

void
API::handleWebSocketClosed(const boost::asio::ip::tcp::socket* socket)
{
  m_wsData.erase(socket);
}

bool
API::sendError(WSData& d, const std::string& str)
{
  d.answer.clear();
  d.answer.put("type", "error");
  d.answer.put("message", str);
  return d.cb(d.session, d.answer);
}

bool
API::handleInjectedCNFTreeNode(WSData& d,
                               Path p,
                               CNFTree::State state,
                               int64_t remote)
{
  char strPath[CNFTree::maxPathDepth + 1];
  CNFTree::pathToStr(p, strPath);
  auto& a = d.answer;
  a.clear();
  a.put("type", "cnftree-update");
  a.put("path", std::string(strPath, CNFTree::getDepth(p)));
  a.put("state", CNFTreeStateToStr(state));
  a.put("remote", std::to_string(remote));
  return d.cb(d.session, a);
}

void
API::conditionalEraseConn(const boost::asio::ip::tcp::socket* socket,
                          bool erase)
{
  if(!erase) {
    m_wsData.erase(socket);
  }
}
}
}

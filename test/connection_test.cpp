#include <boost/asio/io_service.hpp>
#include <boost/asio/ip/address_v4.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/placeholders.hpp>
#include <catch2/catch.hpp>
#include <chrono>
#include <paracooba/cluster-node-store.hpp>
#include <paracooba/config.hpp>
#include <paracooba/net/connection.hpp>
#include <paracooba/networked_node.hpp>

#include <paracooba/messages/jobdescription_receiver.hpp>
#include <paracooba/messages/message.hpp>
#include <paracooba/messages/message_receiver.hpp>

using namespace paracooba;
using namespace paracooba::net;
using namespace paracooba::messages;

class MsgReceiver : public MessageReceiver
{
  public:
  virtual void receiveMessage(const Message& msg, NetworkedNode& nn)
  {
    messages.push(msg);
  }
  std::stack<Message> messages;
};

class JDReceiver : public JobDescriptionReceiver
{
  public:
  virtual void receiveJobDescription(int64_t sentFromID,
                                     JobDescription&& jd,
                                     NetworkedNode& nn)
  {}
};

class JDReceiverProvider : public JobDescriptionReceiverProvider
{
  public:
  virtual JDReceiver* getJobDescriptionReceiver(int64_t subject)
  {
    auto it = receivers.find(subject);
    if(it == receivers.end())
      return nullptr;
    return &it->second;
  }

  private:
  std::map<int64_t, JDReceiver> receivers;
};

class ClNodeStore : public ClusterNodeStore
{
  public:
  virtual const ClusterNode& getNode(int64_t id) const {};
  virtual ClusterNode& getNode(int64_t id){};
  virtual ClusterNodeCreationPair getOrCreateNode(ID id){};
  virtual bool hasNode(ID id) { return false; };
};

TEST_CASE("Initiate a paracooba::net::Connection")
{
  boost::asio::io_service ioService;
  ConfigPtr config1 = std::make_shared<Config>();
  config1->parseParameters();
  ConfigPtr config2 = std::make_shared<Config>();
  config2->parseParameters();

  config1->set(Config::Id, static_cast<int64_t>(1));
  config2->set(Config::Id, static_cast<int64_t>(2));

  LogPtr log1 = std::make_shared<Log>(config1);
  LogPtr log2 = std::make_shared<Log>(config2);

  MsgReceiver msgReceiver1;
  JDReceiverProvider jdReceiverProvider1;

  MsgReceiver msgReceiver2;
  JDReceiverProvider jdReceiverProvider2;

  ClNodeStore clusterNodeStore;

  Connection conn1(ioService,
                   log1,
                   config1,
                   clusterNodeStore,
                   msgReceiver1,
                   jdReceiverProvider1);
  Connection conn2(ioService,
                   log2,
                   config2,
                   clusterNodeStore,
                   msgReceiver2,
                   jdReceiverProvider2);

  NetworkedNode nn(boost::asio::ip::udp::endpoint(
                     boost::asio::ip::address_v4::loopback(), 17170),
                   10);
  auto localEndpoint = boost::asio::ip::tcp::endpoint(
    boost::asio::ip::address_v4::loopback(), 17171);

  nn.setRemoteTcpEndpoint(localEndpoint);

  boost::asio::ip::tcp::acceptor acceptor(ioService, localEndpoint);
  acceptor.async_accept(
    conn1.socket(),
    boost::bind(
      &Connection::readHandler, conn1, boost::asio::placeholders::error, 0));
  conn2.connect(nn);

  ioService.run_for(std::chrono::milliseconds(1));

  REQUIRE(conn1.getRemoteId() == config2->getInt64(Config::Id));
  REQUIRE(conn2.getRemoteId() == config1->getInt64(Config::Id));

  REQUIRE(conn1.isConnectionEstablished());
  REQUIRE(conn2.isConnectionEstablished());

  Message msg1;
  NodeStatus nodeStatusMsg1(10);
  msg1.insert(std::move(nodeStatusMsg1));
  conn1.sendMessage(msg1);

  REQUIRE(msgReceiver2.messages.size() == 0);
  REQUIRE(conn1.getSendMode() == Connection::TransmitControlMessage);

  ioService.run_for(std::chrono::milliseconds(1));

  REQUIRE(msgReceiver2.messages.size() == 1);
  REQUIRE(msgReceiver2.messages.top().getNodeStatus().getWorkQueueSize() ==
          nodeStatusMsg1.getWorkQueueSize());

  Message msg2;
  NodeStatus nodeStatusMsg2(10);
  msg2.insert(std::move(nodeStatusMsg2));
  conn2.sendMessage(msg2);

  REQUIRE(msgReceiver1.messages.size() == 0);

  ioService.run_for(std::chrono::milliseconds(1));

  REQUIRE(msgReceiver1.messages.size() == 1);
  REQUIRE(msgReceiver1.messages.top().getNodeStatus().getWorkQueueSize() ==
          nodeStatusMsg2.getWorkQueueSize());

  for(size_t i = 0; i < 5; ++i) {
    conn2.sendMessage(msg2);
  }

  ioService.run_for(std::chrono::milliseconds(2));

  REQUIRE(msgReceiver1.messages.size() == 6);
}

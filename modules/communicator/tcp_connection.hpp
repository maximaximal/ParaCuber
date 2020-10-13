#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <queue>
#include <variant>

#include <boost/asio/ip/tcp.hpp>

#include <paracooba/common/status.h>

struct parac_message;
struct parac_file;
struct parac_compute_node;

namespace boost::system {
class error_code;
}

namespace parac::communicator {
class Service;
class PacketHeader;

/** @brief Class representing a connection between two compute nodes.
 *
 * This class handles the complete connection live-cycle. Connections are
 * established over TCP sockets. If a socket is ended, it is tried to reconnect
 * to the remote host.
 */
class TCPConnection {
  public:
  enum Lifecycle { Initializing, Active, Dead };
  enum TransmitMode {
    TransmitInit,
    TransmitMessage,
    TransmitFile,
    TransmitEnd
  };
  enum ResumeMode { EndAfterShutdown, RestartAfterShutdown };
  struct EndTag;
  struct ACKTag;

  /** @brief Initialize a paracooba connection on an opened socket.
   */
  explicit TCPConnection(Service& service,
                         boost::asio::ip::tcp::socket&& socket,
                         int connectionTry = 0);
  ~TCPConnection();

  void send(parac_message& message);
  void send(parac_file& file);
  void send(EndTag& end);
  void send(parac_message&& message);
  void send(parac_file&& file);
  void send(EndTag&& end);
  void sendACK(uint32_t id, parac_status status);

  private:
  struct InitiatorMessage;
  struct SendQueueEntry;
  struct State;
  std::shared_ptr<State> m_state;

  void send(SendQueueEntry&& e);

  void readHandler(boost::system::error_code ec, size_t bytes_received);
  void writeHandler(boost::system::error_code ec, size_t bytes_sent);

  bool shouldHandlerBeEnded();

  bool handleInitiatorMessage(const InitiatorMessage& init);
  bool handleReceivedACK(const PacketHeader& ack);
  void handleReceivedMessage();
  void handleReceivedFileStart();
  void handleReceivedFileChunk();
  void handleReceivedFile();

  static void compute_node_free_func(parac_compute_node* n);
  static void compute_node_func(parac_compute_node* n);
  static void compute_node_send_message_to_func(parac_compute_node* n,
                                                parac_message* msg);
  static void compute_node_send_file_to_func(parac_compute_node* n,
                                             parac_file* file);
};

constexpr const char*
ConnectionResumeModeToStr(TCPConnection::ResumeMode resumeMode) {
  switch(resumeMode) {
    case TCPConnection::ResumeMode::EndAfterShutdown:
      return "EndAfterShutdown";
    case TCPConnection::ResumeMode::RestartAfterShutdown:
      return "RestartAfterShutdown";
  }
  return "Unknown Resume Mode";
}

std::ostream&
operator<<(std::ostream& o, TCPConnection::ResumeMode resumeMode);

}
#include "../../include/paracooba/messages/jobdescription_transmitter.hpp"
#include "../../include/paracooba/networked_node.hpp"

namespace paracooba {
namespace messages {
void
JobDescriptionTransmitter::transmitJobDescription(JobDescription&& jd,
                                                  NetworkedNode& nn,
                                                  SuccessCB sendFinishedCB)
{
  nn.transmitJobDescription(std::move(jd), nn, sendFinishedCB);
}
}
}

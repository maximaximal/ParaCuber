#ifndef PARACOOBA_MESSAGES_JOBDESCRIPTION_TRANSMITTER
#define PARACOOBA_MESSAGES_JOBDESCRIPTION_TRANSMITTER

#include <functional>

#include "../types.hpp"

namespace paracooba {
class NetworkedNode;

namespace messages {
class JobDescription;

class JobDescriptionTransmitter
{
  public:
  virtual void transmitJobDescription(JobDescription&& jd,
                                      NetworkedNode& nn,
                                      SuccessCB sendFinishedCB);
};
}
}

#endif

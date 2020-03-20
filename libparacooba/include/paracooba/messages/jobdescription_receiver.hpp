#ifndef PARACOOBA_MESSAGES_JOBDESCRIPTION_RECEIVER
#define PARACOOBA_MESSAGES_JOBDESCRIPTION_RECEIVER

namespace paracooba {
class NetworkedNode;

namespace messages {
class JobDescription;

class JobDescriptionReceiver
{
  public:
  virtual void receiveJobDescription(int64_t sentFromID,
                                     JobDescription&& jd,
                                     NetworkedNode& nn) = 0;
};

/** @brief Provide a JobDescriptionReceiver instance to handle incoming job
 * descriptions. */
class JobDescriptionReceiverProvider
{
  public:
  virtual JobDescriptionReceiver* getJobDescriptionReceiver(
    int64_t subject) = 0;
};
}
}

#endif

#ifndef PARACOOBA_CUBER_PREGENERATED_HPP
#define PARACOOBA_CUBER_PREGENERATED_HPP

#include "../log.hpp"
#include "cuber.hpp"
#include "paracooba/messages/job_initiator.hpp"
#include <vector>

namespace paracooba {
namespace cuber {
class Pregenerated : public Cuber
{
  public:
  explicit Pregenerated(ConfigPtr config,
                        LogPtr log,
                        CNF& rootCNF,
                        const messages::JobInitiator& ji);
  virtual ~Pregenerated();

  bool init();
  const messages::JobInitiator& getJobInitiator() { return m_ji; }

  virtual bool shouldGenerateTreeSplit(Path path);
  virtual bool getCube(Path path, std::vector<int>& literals);

  private:
  size_t m_counter = 0;
  messages::JobInitiator m_ji;

  const std::vector<int> *m_cubesFlatVector;
  std::vector<size_t> m_cubesJumpList;
  size_t m_normalizedPathLength = 0;
};
}
}

#endif

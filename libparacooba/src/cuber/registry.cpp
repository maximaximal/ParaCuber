#include "../../include/paracooba/cuber/registry.hpp"
#include "../../include/paracooba/cuber/cuber.hpp"
#include "../../include/paracooba/cuber/literal_frequency.hpp"
#include "../../include/paracooba/cuber/pregenerated.hpp"
#include "paracooba/messages/job_initiator.hpp"

namespace paracooba {
namespace cuber {
Registry::Registry(ConfigPtr config, LogPtr log, CNF& rootCNF)
  : Cuber(config, log, rootCNF)
  , m_logger(log->createLogger("Registry"))
{}
Registry::~Registry() {}

bool
Registry::init(Mode mode, const messages::JobInitiator* ji)
{
  m_cubers.clear();
  m_mode = mode;

  switch(mode) {
    case LiteralFrequency: {
      auto litFreqPtr = std::make_unique<cuber::LiteralFrequency>(
        m_config, m_log, m_rootCNF, &m_allowanceMap);
      if(!litFreqPtr->init()) {
        return false;
      }
      cuber::LiteralFrequency& litFreq = *litFreqPtr;
      m_cubers.push_back(std::move(litFreqPtr));

      for(auto& cuber : m_cubers) {
        cuber->m_allowanceMap = litFreq.getLiteralFrequency();
      }
      break;
    }
    case PregeneratedCubes: {
      assert(ji);
      auto pregeneratedCubesPtr =
        std::make_unique<cuber::Pregenerated>(m_config, m_log, m_rootCNF, *ji);
      if(!pregeneratedCubesPtr->init()) {
        return false;
      }
      m_jobInitiator = &pregeneratedCubesPtr->getJobInitiator();
      m_cubers.push_back(std::move(pregeneratedCubesPtr));
      break;
    }
  }

  // Now, the allowance map is ready and all waiting callbacks can be called.
  allowanceMapWaiter.setReady(&m_allowanceMap);
  return true;
}

Cuber&
Registry::getActiveCuber() const
{
  return *m_cubers[0];
}

bool
Registry::shouldGenerateTreeSplit(Path path)
{
  return getActiveCuber().shouldGenerateTreeSplit(path);
}
bool
Registry::getCube(Path path, Cube& literals)
{
  return getActiveCuber().getCube(path, literals);
}
}
}

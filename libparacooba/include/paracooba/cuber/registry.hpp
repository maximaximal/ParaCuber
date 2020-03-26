#ifndef PARACOOBA_CUBER_REGISTRY_HPP
#define PARACOOBA_CUBER_REGISTRY_HPP

#include "../cnftree.hpp"
#include "../log.hpp"
#include "../readywaiter.hpp"
#include "../types.hpp"
#include "cuber.hpp"
#include <memory>
#include <optional>
#include <queue>
#include <vector>

namespace paracooba {
class CNF;

namespace messages {
class JobInitiator;
}

namespace cuber {
class Registry : public Cuber
{
  public:
  explicit Registry(ConfigPtr config, LogPtr log, CNF& rootCNF);
  ~Registry();

  enum Mode
  {
    PregeneratedCubes,
    LiteralFrequency
  };

  bool init(Mode mode, const messages::JobInitiator* ji = nullptr);

  using CuberVector = std::vector<std::unique_ptr<Cuber>>;
  using AllowanceMap = LiteralMap;

  Cuber& getActiveCuber() const;
  bool generateCube(Path path, Literal& var);
  inline AllowanceMap& getAllowanceMap() { return m_allowanceMap; }

  Mode getCuberMode() { return m_mode; }

  const messages::JobInitiator* getJobInitiator() { return m_jobInitiator; }

  ReadyWaiter<AllowanceMap> allowanceMapWaiter;

  virtual bool shouldGenerateTreeSplit(Path path);
  virtual bool getCube(Path path, Cube& literals);

  private:
  Logger m_logger;
  CuberVector m_cubers;

  AllowanceMap m_allowanceMap;
  Mode m_mode = LiteralFrequency;
  const messages::JobInitiator* m_jobInitiator = nullptr;
};
}
}

#endif

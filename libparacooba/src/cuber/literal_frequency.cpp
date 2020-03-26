#include "../../include/paracooba/cuber/literal_frequency.hpp"
#include "../../include/paracooba/cadical_task.hpp"
#include "../../include/paracooba/cnf.hpp"
#include "../../include/paracooba/communicator.hpp"
#include "../../include/paracooba/runner.hpp"
#include "../../include/paracooba/util.hpp"
#include <cadical/cadical.hpp>
#include <vector>

namespace paracooba {
namespace cuber {

class ClauseIterator : public CaDiCaL::ClauseIterator
{
  public:
  explicit ClauseIterator(Cuber::LiteralOccurenceMap& m)
    : m_m(m)
  {}
  ~ClauseIterator() {}

  virtual bool clause(const std::vector<int>& clause)
  {
    for(int l : clause) {
      m_m[FastAbsolute(l)].count += 1;
    }
    return true;
  }

  private:
  Cuber::LiteralOccurenceMap& m_m;
};

LiteralFrequency::LiteralFrequency(ConfigPtr config,
                                   LogPtr log,
                                   CNF& rootCNF,
                                   LiteralMap* allowanceMap)
  : Cuber(config, log, rootCNF)
  , m_literalFrequency(allowanceMap)
{}
LiteralFrequency::~LiteralFrequency() {}

bool
LiteralFrequency::init()
{
  // Frequency map only needs to be built on the client.
  if(m_config->isDaemonMode()) {
    return true;
  }

  *m_literalFrequency =
    LiteralMap(m_rootCNF.getRootTask()->getVarCount() + 1, 0);

  LiteralOccurenceMap occurenceMap;
  initLiteralOccurenceMap(occurenceMap, m_literalFrequency->size());

  ClauseIterator it(occurenceMap);
  PARACOOBA_LOG(m_logger, Trace) << "Begin traversing CNF clauses for naive "
                                    "cutter literal frequency map. Map Size: "
                                 << m_literalFrequency->size() << " elements.";
  m_rootCNF.getRootTask()->getSolver().traverse_clauses(it);
  PARACOOBA_LOG(m_logger, Trace)
    << "Finished traversing CNF clauses for"
       " literal frequency map. The map size in RAM is "
    << BytePrettyPrint(occurenceMap.size() * sizeof(occurenceMap[0]))
    << ". Sorting by value now.";

  literalOccurenceMapToLiteralMap(*m_literalFrequency, std::move(occurenceMap));

  PARACOOBA_LOG(m_logger, Trace)
    << "Finished sorting CNF clauses by frequency. Reduced Map size ready for "
       "transmission: "
    << BytePrettyPrint(m_literalFrequency->size() *
                       sizeof((*m_literalFrequency)[0]));
  return true;
}

bool
LiteralFrequency::shouldGenerateTreeSplit(Path path)
{
  assert(m_literalFrequency);
  assert(m_literalFrequency->size() > 0);

  if(CNFTree::getDepth(path) > CNFTree::maxPathDepth) {
    return false;
  }
  auto additionComponent = getAdditionComponent(path);
  auto moduloComponent = getModuloComponent(path);

  auto dec = additionComponent + (moduloComponent - 1);

  if(((float)dec / (float)m_literalFrequency->size()) >=
     m_config->getFloat(Config::FreqCuberCutoff)) {
    return false;
  }

  // The next decision is always determined by the addition component, as no
  // decision must be made. The next decision is therefore always the next most
  // frequent literal.

  if(dec >= m_literalFrequency->size()) {
    return false;
  }

  return true;
}

bool
LiteralFrequency::getCube(Path path, Cube& literals)
{
  assert(m_literalFrequency);
  assert(m_literalFrequency->size() > 0);

  literals.clear();
  literals.reserve(CNFTree::getDepth(path));

  for(size_t depth = 1; depth <= CNFTree::getDepth(path); ++depth) {
    Path currPath = CNFTree::setDepth(path, depth);

    auto additionComponent = getAdditionComponent(currPath);
    auto moduloComponent = getModuloComponent(currPath);
    auto dec = additionComponent + (moduloComponent - 1);

    auto lit = (*m_literalFrequency)[dec];
    assert(lit != 0);

    literals.push_back(lit * (CNFTree::getAssignment(path, depth) ? 1 : -1));
  }

  return true;
}
}
}

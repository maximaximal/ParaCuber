#ifndef PARACOOBA_CUBER_CUBER_HPP
#define PARACOOBA_CUBER_CUBER_HPP

#include "../cnftree.hpp"
#include "../log.hpp"
#include <memory>

namespace paracooba {
class CNF;

namespace cuber {

/** @brief Base class for all cubing algorithms.
 *
 * This class is initialised once per formula (daemon and client), so the
 * algorithm can track internal statistics to improve cube selection if desired.
 */
class Cuber
{
  public:
  explicit Cuber(ConfigPtr config, LogPtr log, CNF& rootCNF);
  virtual ~Cuber();

  struct LiteralOccurence
  {
    LiteralOccurence(Literal literal = 0)
      : literal(literal)
    {}

    Literal literal;
    size_t count = 0;

    void operator=(Literal lit) { this->literal = lit; }

    bool operator<(const LiteralOccurence& o) { return count < o.count; }
    bool operator>(const LiteralOccurence& o) { return count > o.count; }
  };
  using LiteralOccurenceMap = std::vector<LiteralOccurence>;

  /** @brief Checks if a tree split should be generated.
   */
  virtual bool shouldGenerateTreeSplit(Path path) = 0;
  /** @brief Returns the full cube for a path. Return false means the cube does
   * not need to be solved and is UNSAT.
   */
  virtual bool getCube(Path path, std::vector<int>& literals) = 0;

  static inline uint64_t getModuloComponent(Path p)
  {
    return (1 << (CNFTree::getDepth(p)));
  }
  static inline uint64_t getAdditionComponent(Path p)
  {
    return CNFTree::getDepthShiftedPath(p);
  }

  /** @brief Sorts and converts the given LiteralOccurenceMap to a given
   * LiteralMap.
   */
  static void literalOccurenceMapToLiteralMap(LiteralMap& target,
                                              LiteralOccurenceMap&& source);

  static void initLiteralOccurenceMap(LiteralOccurenceMap& map, size_t n);

  protected:
  friend class Registry;

  ConfigPtr m_config;
  LogPtr m_log;
  Logger m_logger;
  CNF& m_rootCNF;

  LiteralMap* m_allowanceMap = nullptr;

  private:
};
}
}

#endif

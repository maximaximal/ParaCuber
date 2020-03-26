#include "../../include/paracooba/cuber/cuber.hpp"
#include <algorithm>
#include <iterator>
#include <numeric>

namespace paracooba {
namespace cuber {
Cuber::Cuber(ConfigPtr config, LogPtr log, CNF& rootCNF)
  : m_config(config)
  , m_log(log)
  , m_logger(log->createLogger("Cuber"))
  , m_rootCNF(rootCNF)
{}
Cuber::~Cuber() {}

void
Cuber::literalOccurenceMapToLiteralMap(LiteralMap& target,
                                       LiteralOccurenceMap&& source)
{
  target.clear();
  target.reserve(source.size());

  std::sort(source.begin(), source.end(), std::greater<>());

  std::transform(source.begin(),
                 source.end(),
                 std::back_inserter(target),
                 [](const LiteralOccurence& occ) -> Literal {
                   return occ.literal;
                 });
}

void
Cuber::initLiteralOccurenceMap(LiteralOccurenceMap& map, size_t n)
{
  map.resize(n);
  std::iota(map.begin(), map.end(), 1);
}
}
}

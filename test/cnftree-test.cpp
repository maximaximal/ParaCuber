#include <catch2/catch.hpp>
#include <paracooba/cnftree.hpp>

using namespace paracooba;

TEST_CASE("CNFTree::Path manipulation")
{
  Path p = 0;
  REQUIRE(CNFTree::getDepth(p) == 0);
  REQUIRE(CNFTree::getPath(p) == 0);

  p = CNFTree::setDepth(p, 1);
  REQUIRE(CNFTree::getDepth(p) == 1);
  REQUIRE(CNFTree::getPath(p) == 0);

  p = CNFTree::setDepth(p, 2);
  REQUIRE(!CNFTree::getAssignment(p, 1));
  REQUIRE(!CNFTree::getAssignment(p, 2));
  REQUIRE(!CNFTree::getAssignment(p, 3));

  p = CNFTree::buildPath(0, 2);
  REQUIRE(CNFTree::getPath(p) == 0);
  p = CNFTree::buildPath(0xFFFF0000FFFF0000u, 2);
  REQUIRE(CNFTree::getPath(p) == 0xFFFF0000FFFF0000u);
  p = CNFTree::buildPath((uint8_t)0b00000001u, 2);
  REQUIRE(CNFTree::getPath(p) == ((uint64_t)0b00000001u) << (64 - 8));
  REQUIRE(!CNFTree::getAssignment(p, 1));
  p = CNFTree::buildPath((uint8_t)0b10000000u, 2);
  REQUIRE(CNFTree::getPath(p) == ((uint64_t)0b10000000u) << (64 - 8));

  REQUIRE(CNFTree::getDepth(p) == 2);
  REQUIRE(CNFTree::getAssignment(p, 1));
  REQUIRE(!CNFTree::getAssignment(p, 2));
  REQUIRE(CNFTree::getDepthShiftedPath(p) == (uint64_t)0b00000010uL);

  p = CNFTree::buildPath((uint8_t)0b10000000u, 4);
  REQUIRE(CNFTree::getDepthShiftedPath(p) == (uint64_t)0b00001000uL);
  REQUIRE(CNFTree::getDepthShiftedPath(p) == 8u);

  REQUIRE(CNFTree::getDepth(CNFTree::getNextLeftPath(p)) == 5);
  REQUIRE(!CNFTree::getAssignment(CNFTree::getNextLeftPath(p), 5));
  REQUIRE(CNFTree::getAssignment(CNFTree::getNextRightPath(p), 5));
  REQUIRE(CNFTree::getDepthShiftedPath(CNFTree::getNextLeftPath(p)) ==
          (uint64_t)0b00010000uL);
  REQUIRE(CNFTree::getDepthShiftedPath(CNFTree::getNextRightPath(p)) ==
          (uint64_t)0b00010001uL);

  p = CNFTree::buildPath((uint32_t)0xFFFFFFF0u, 32);
  REQUIRE(CNFTree::getAssignment(p, 15));
  REQUIRE(CNFTree::getAssignment(p, 16));
  REQUIRE(CNFTree::getAssignment(p, 20));
  REQUIRE(CNFTree::getAssignment(p, 25));
  REQUIRE(!CNFTree::getAssignment(p, 31));

  p = CNFTree::buildPath((uint32_t)0xFFFFFFFFu, 16);
  REQUIRE(CNFTree::getDepth(p) == 16);
  REQUIRE(CNFTree::getDepth(p) - 1 == 15);
  REQUIRE(CNFTree::getDepth(CNFTree::setDepth(p, 15)) == 15);
  REQUIRE(CNFTree::getDepth(CNFTree::setDepth(p, CNFTree::getDepth(p) - 1)) == 15);
  REQUIRE(CNFTree::getDepth(CNFTree::getParent(p)) == 15);

  // This is also a test for cuber::Cuber::getAdditionComponent. The behaviour
  // stays the same.
  p = CNFTree::buildPath((uint8_t)0b11110000u, 4);
  REQUIRE(CNFTree::getDepthShiftedPath(p) == 15u);
  REQUIRE(CNFTree::maxPathDepth == 58u);

  p = CNFTree::setDepth(CNFTree::setAssignment(p, 5, true), 5);
  REQUIRE(CNFTree::getPath(p) == ((uint64_t)0b11111000u) << (64 - 8));
  REQUIRE(CNFTree::getDepthShiftedPath(p) == 31u);
}

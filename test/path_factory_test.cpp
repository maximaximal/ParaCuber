#include "paracooba/task_factory.hpp"
#include "paracooba/util.hpp"
#include <catch2/catch.hpp>
#include <paracooba/cnftree.hpp>
#include <paracooba/config.hpp>
#include <paracooba/decision_task.hpp>
#include <paracooba/log.hpp>

using namespace paracooba;

TEST_CASE("TaskFactory Manipulation")
{
  ConfigPtr config = std::make_shared<Config>();
  char* argv[] = { (char*)"", NULL };
  config->parseParameters(1, argv);
  LogPtr log = std::make_shared<Log>(config);

  TaskFactory factory(config, log, nullptr);

  Path p = CNFTree::buildPath(static_cast<uint8_t>(0b01010101), 8);

  REQUIRE(!factory.canProduceTask());
  REQUIRE(!factory.produceTask().task);
  factory.addPath(p, TaskFactory::Mode::CubeOrSolve, 0);
  REQUIRE(factory.canProduceTask());
  auto produced = factory.produceTask();
  REQUIRE(produced.originator == 0);
  REQUIRE(produced.task);

  auto decisionTask =
    static_unique_pointer_cast<DecisionTask>(std::move(produced.task));
  REQUIRE(decisionTask->getPath() == p);
}

TEST_CASE("PathFactory Automatic Priority Sorting")
{
  ConfigPtr config = std::make_shared<Config>();
  char* argv[] = { (char*)"", NULL };
  config->parseParameters(1, argv);
  LogPtr log = std::make_shared<Log>(config);

  TaskFactory factory(config, log, nullptr);

  Path p1 = CNFTree::buildPath(static_cast<uint8_t>(0b01010101), 8);
  Path p2 = CNFTree::buildPath(static_cast<uint8_t>(0b01010101), 7);
  factory.addPath(p1, TaskFactory::Mode::CubeOrSolve, 0);
  factory.addPath(p2, TaskFactory::Mode::CubeOrSolve, 0);
  REQUIRE(factory.canProduceTask());
  auto produced = factory.produceTask();
  REQUIRE(produced.originator == 0);
  REQUIRE(produced.task);
  auto decisionTask =
    static_unique_pointer_cast<DecisionTask>(std::move(produced.task));
  REQUIRE(decisionTask->getPath() == p2);
}

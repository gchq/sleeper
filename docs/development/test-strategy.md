Test Strategy
=============

The Java code includes unit tests, integration tests and system tests. We use JUnit 5, with AssertJ for assertions.

The Rust code includes unit tests and integration tests. We use the built-in test framework.

We also have a manual testing setup that combines system test tools with a deployed instance of Sleeper, documented in
the [system tests guide](system-tests.md#manual-testing).

We'll establish definitions of the different types of test, and talk about when and how we use them.

### Definitions

A unit test is any test that runs entirely in-memory without any I/O operations (eg. file system or network calls).
If you configure your IDE to run all unit tests at once, they should finish in less than a minute. The unit of a test
should be a particular behaviour or scenario, rather than eg. a specific method.

A system test is a test that works with a deployed instance of Sleeper. These are JUnit tests found in the
module `system-test/system-test-suite`. They use the class `SleeperSystemTest` as the entry point to work with an
instance of Sleeper. This is the acceptance test suite we use to define releasability of the system. This is documented
in the [system tests guide](system-tests.md#acceptance-tests).

An integration test is any test which does not meet the definition of a unit test or a system test. Usually it uses
external dependencies with Testcontainers, tests network calls with WireMock, or uses the local file system.

### Strategy

We always prefer to provide test coverage with in-memory unit tests. We use integration tests when it is not possible to
test a behaviour with unit tests, or when faking or mocking I/O operations would either couple the tests too directly to
the I/O technology, or would exclude all logic from the test.

All code should be covered with unit tests or integration tests, with a few exceptions:

- Deployment code in CDK, until we find a good way to test this
- Experimental features that were implemented without TDD
  - TDD is preferred in all cases
  - Tests should be added as soon as possible

We also write system tests for all features that are not experimental. We keep the number of system tests per feature
low, as these tests are relatively slow to run. They should verify that each feature is functional when deployed in AWS,
and test properties such as performance and throughput. When we add a new feature, we add one or two simple cases to
this test suite, as a complement to more detailed unit testing.

### Design techniques

We use tests to define the behaviour of the system, writing all tests in the style of behaviour driven development
(BDD), as defined by Dan North in the following article: https://dannorth.net/introducing-bdd/

We avoid mocking wherever possible, and prefer to use test fakes, e.g. implement an interface to a database with a
wrapper around a HashMap.

We use test helper methods to make tests as readable as possible, and as close as possible to a set of English
given/when/then statements.

We try to use test driven development (TDD) whenever possible, but we do not require this of all contributors. We use
the definitions of this found in the following resources:

- Uncle Bob's Three Rules of TDD: http://www.butunclebob.com/ArticleS.UncleBob.TheThreeRulesOfTdd
- Kent Beck's Canon TDD: https://tidyfirst.substack.com/p/canon-tdd

### JUnit test classes

Unit tests should be in a class ending with Test, like MyFeatureTest. Integration tests should be in a class ending with
IT, like MyFeatureIT. Classes named this way will be picked up by Maven's Surefire plugin for unit tests, and Failsafe
for integration tests.

System tests should be in a class ending with ST, like CompactionPerformanceST, and must be tagged with the annotation
`SystemTest`. This means they will only be run as part of a system test suite, or directly. See
the [system tests guide](system-tests.md#acceptance-tests).

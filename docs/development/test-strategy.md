Test Strategy
=============

The Maven project includes unit tests, integration tests and system tests. We use JUnit 5, with AssertJ for assertions.

We also have a manual testing setup that combines system test tools with a deployed instance of Sleeper, documented in
the [system tests guide](system-tests.md#manual-testing).

### Definitions

A unit test is any test that runs entirely in-memory without any I/O operations (eg. file system or network calls).
If you configure your IDE to run all unit tests at once, they should finish in less than a minute. The unit of a test
should be a particular behaviour or scenario, rather than eg. a specific method.

A system test is a test that works with a deployed instance of Sleeper. These can be found in the
module `system-test/system-test-suite`. They use the class `SleeperSystemTest` as the entry point to work with an
instance of Sleeper. This is the acceptance test suite we use to define releasability of the system. This is documented
in the [system tests guide](system-tests.md#acceptance-tests). If you add a new feature, please add one or
two simple cases to this test suite, as a complement to more detailed unit testing.

An integration test is any test which does not meet the definition of a unit test or a system test. Usually it uses
external dependencies with Testcontainers, tests network calls with WireMock, or uses the local file system.

### Implementation

Unit tests should be in a class ending with Test, like MyFeatureTest. Integration tests should be in a class ending with
IT, like MyFeatureIT. Classes named this way will be picked up by Maven's Surefire plugin for unit tests, and Failsafe
for integration tests.

System tests should be in a class ending with ST, like CompactionPerformanceST, and must be tagged with the annotation
`SystemTest`. This means they will only be run as part of a system test suite, or directly. See
the [system tests guide](system-tests.md#acceptance-tests).

We avoid mocking wherever possible, and prefer to use test fakes, eg. implement an interface to a database with a
wrapper around a HashMap. Use test helper methods to make tests as readable as possible, and as close as possible to a
set of English given/when/then statements.

Test Strategy
=============

The Java code includes unit tests, integration tests and system tests. We use JUnit 5, with AssertJ for assertions.

The Rust code includes unit tests and integration tests. We use the built-in test framework.

The Python code includes unit tests and integration tests with Pytest.

Our system tests include end to end testing of the system, as well as tests of performance, scaling, throughput.

We have a manual testing setup that combines system test tools with a deployed instance of Sleeper, documented in
the [system tests guide](system-tests.md#manual-testing).

We have a separate article on [test design techniques](test-design.md) that we try to apply uniformly across the
codebase. These should be followed as part of the test strategy.

We'll establish definitions of the different types of test, and talk about when and how we use them.

### Definitions

A unit test is any test that runs entirely in-memory without any I/O operations (e.g. file system or network calls).
If you configure your IDE to run all Sleeper's unit tests, they should finish in less than a minute. The unit of a test
should be a particular behaviour or scenario, rather than e.g. a specific method.

A system test is a test that works with an instance of Sleeper deployed on AWS. These are JUnit tests found in the
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
  - Test coverage should be improved as soon as possible

We also write system tests for all features that are not experimental. This is our system test suite in JUnit. We keep
the number of system tests per feature low, as these tests are relatively slow to run. They should verify that each
feature is functional when deployed in AWS, and test properties such as performance and throughput. When we add a new
feature, we add some simple coverage to verify it still works when deployed, at most one or two cases as a complement
to more detailed unit testing. This should cover things like problems with permissions or networking configuration, that
would only show up when deployed.

We consider performance testing when we want to provide guarantees about the scalability or performance of the system.
We also use this to try to discover any differences in behaviour at scale, or bugs that only show up with a large amount
of data or processes. This is included in our JUnit system test suite.

We try to avoid needing manual testing. If any behaviour in the system breaks, we want to know about it from one of the
tests described above. Manual testing should usually be purely exploratory.

### JUnit test classes

Unit tests should be in a class ending with Test, like MyFeatureTest. Integration tests should be in a class ending with
IT, like MyFeatureIT. Classes named this way will be picked up by Maven's Surefire plugin for unit tests, and Failsafe
for integration tests.

System tests should be in a class ending with ST, like CompactionPerformanceST, and must be tagged with the annotation
`SystemTest`. This means they will only be run as part of a system test suite, or directly. They may also be tagged
with an annotation for which test suite they should run in, which will default to the quick test suite. See
the [system tests guide](system-tests.md#acceptance-tests).

For assertions we use [AssertJ](https://assertj.github.io/doc/) instead of JUnit's built-in assertions. We also have
some approval tests with [ApprovalTests.Java](https://github.com/approvals/ApprovalTests.Java).

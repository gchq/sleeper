Test Strategy
=============

The Java code includes unit tests, integration tests and system tests. We use JUnit 5, with AssertJ for assertions.

The Rust code includes unit tests and integration tests. We use the built-in test framework.

The Python code includes unit tests and integration tests with Pytest.

We also have a manual testing setup that combines system test tools with a deployed instance of Sleeper, documented in
the [system tests guide](system-tests.md#manual-testing).

We'll establish definitions of the different types of test, and talk about when and how we use them. We'll look at
test design techniques that we try to apply uniformly across the codebase.

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

We also write system tests for all features that are not experimental. We keep the number of system tests per feature
low, as these tests are relatively slow to run. They should verify that each feature is functional when deployed in AWS,
and test properties such as performance and throughput. When we add a new feature, we add one or two simple cases to
this test suite, as a complement to more detailed unit testing.

### JUnit test classes

Unit tests should be in a class ending with Test, like MyFeatureTest. Integration tests should be in a class ending with
IT, like MyFeatureIT. Classes named this way will be picked up by Maven's Surefire plugin for unit tests, and Failsafe
for integration tests.

System tests should be in a class ending with ST, like CompactionPerformanceST, and must be tagged with the annotation
`SystemTest`. This means they will only be run as part of a system test suite, or directly. See
the [system tests guide](system-tests.md#acceptance-tests).

### Style

Our test names should be an English sentence starting with "should", that describes the behaviour we wish to assert.
Most tests should be split into given/when/then sections, with comments separating each section. Those can sometimes be
combined but we prefer to separate them explicitly for larger tests. We use AssertJ for assertions. Here are some
example tests:

```java
@Test
void shouldReadIntegers() {
    // Given
    String input = "1,2,3";

    // When
    List<Number> numbers = NumberReader.read(input);

    // Then
    assertThat(numbers).containsExactly(1, 2, 3);
}

@Test
void shouldFailIfInputContainsNonNumber() {
    // Given
    String input = "abc";

    // When / Then
    assertThatThrownBy(() -> NumberReader.read(input))
        .isInstanceOf(IllegalArgumentException.class);
}
```

### Test design

We have a number of test design techniques that we try to apply uniformly across the codebase. All of these techniques
have a shared purpose in common, to make it easy for us to change the production code safely. To achieve that we try to
align the tests as closely as possible to a real scenario that a user would care about. That's not always a direct
equivalence in practice, but we always look for ways to do that.

#### Behaviour Driven Design (BDD)

We use tests to define the behaviour of the system, writing all tests in the style of behaviour driven development
(BDD). The following article by Dan North defines this term, though not all examples match our style and conventions:
https://dannorth.net/introducing-bdd/

To us, this means that all tests should define a behaviour from the perspective of a user or consumer of the code. They
should describe a scenario in terms of things the user would care about, and how they would use it. We want to match how
the user will interact with the production code, and the terms they would use. We abstract away from the real
implementation to let us change the design.

Some developers are more used to thinking of integration or acceptance tests in this way, but we apply this to in-memory
unit tests as well. This is important because when a test describes a user-focused scenario, it's less likely to need to
change as the design of the implementation changes. This isn't universal, but it's important as a principle.

#### Test Driven Design (TDD)

We try to use test driven development (TDD) whenever possible, but we do not require this of all contributors. We use
the definitions of this found in the following resources:

- Robert C. Martin's Three Rules of TDD: http://www.butunclebob.com/ArticleS.UncleBob.TheThreeRulesOfTdd
- Kent Beck's Canon TDD: https://tidyfirst.substack.com/p/canon-tdd

#### Ports and adapters

We use ports and adapters architecture as a framing for how to design our code for testability, how to choose
entrypoints for our tests, and how to design test harnesses and swap between real implementations and test fakes.
This was defined by Alistair Cockburn in the following article: https://alistair.cockburn.us/hexagonal-architecture

We use this to define ports which we can use to interact with the core business logic of the system. Adapters are
input/output code that interact with the application core through the ports. Tests can also interact with the system
through the ports. Adapters can be swapped out for in-memory test fakes for tests that aren't about them, to keep the
tests in-memory and fast. We can test adapters through the adapter, still invoking the real application code, and we
have the option to swap out any other necessary adapters for test fakes.

Note that the system contains some modules with "core" in the name, which are mostly core business logic, with no I/O
dependencies except for logging, JSON serialisation/deserialisation, and basic utilities. These modules may include some
file system adapter code whose only dependency is the standard library.

#### Mocking and test fakes

One technique that helps us to keep a user perspective for our unit tests is high quality in-memory test fakes. These
implement the same interface as some input/output code, and behave as closely as possible to the real thing, but hold
the state in memory instead of doing real I/O. We avoid mocking wherever possible, so that we can keep all our tests
behaving and reading the same as the behaviour in production.

For an example of a test fake, say we have a data store backed by a database. Our database code can implement an
interface, and a test fake can implement that interface as a class that's a wrapper around an in-memory collection,
e.g. HashMap or ArrayList. Any queries against the database can also be implemented in-memory in the test fake.

That lets us minimise the tests that need to run against the database. In Sleeper we do this with various data stores,
usually DynamoDB and S3. We can test our DynamoDB and S3 code separately in integration tests against LocalStack, using
Testcontainers.

For a simpler example, when we want to send a message to an SQS queue, we would write an interface with a method to send
the message. We can then implement that as a method reference to the `add` method on a `LinkedList`, and assert against
the contents of that list.

We avoid using Mockito or other mocking frameworks because they force us to couple too tightly to the implementation. By
using an in-memory implementation as a test fake, it makes it much easier to refactor e.g. method signatures that you
would otherwise need to mock.

We use WireMock in cases where we can't use LocalStack, so that we can test real network calls but fake the server when
a real implementation is not available.

#### Entrypoints and helper methods

Tests should call into production code through stable entrypoints, as close as possible to what will be used by a real
user or consumer of the code. We want to make refactoring the production code as easy as possible, so we avoid coupling
tests to anything in the production code that we might want to change.

In particular, where there are constructors that wire in dependencies, or method calls that aren't a core part of the
Sleeper API, or aren't the main way a user would interact with it, we try to avoid calling these directly from tests.
We minimise the number of places in tests that make calls to any element of the production code that may change.

Our main tool for stabilising and managing these test entrypoints is test helper methods. For example, we commonly have
a factory method in a test class to construct the main object under test, or the object we've chosen as the entrypoint
to interact with it. Our test fakes are designed to be as easy as possible to use in this way.

Note that the production code entrypoint for a test is not necessarily the main class under test. Not all classes are
tested directly, and some are tested implicitly by tests for another class. This is fine, although we prefer test
coverage to be in the same module as the class. It can require some reasoning to find which classes are covered by which
tests, but we try to adjust the design and documentation to make this easier.

### Example tests

Below is an example of this based on real Sleeper code. We send requests to SQS to commit a transaction to update a
Sleeper table. A compaction removes some files from a Sleeper table and replaces them with its output file. We combine
the results of multiple compactions into one transaction, and send the transaction to an SQS queue to be committed to
the state store.

```java
public class CompactionCommitBatcherTest {

    private final Queue<StateStoreCommitRequest> queue = new LinkedList<>();

    @Test
    void shouldSendMultipleCompactionCommitsForSameTableAsOneTransaction() {
        // Given two compaction jobs for the same table
        TableProperties table = createTable("test-table");
        CompactionJob job1 = jobFactory(table).createCompactionJobWithFilenames(
                "job1", List.of("file1.parquet"), "root");
        CompactionJob job2 = jobFactory(table).createCompactionJobWithFilenames(
                "job2", List.of("file2.parquet"), "root");
        ReplaceFileReferencesRequest request1 = defaultReplaceFileReferencesRequest(job1);
        ReplaceFileReferencesRequest request2 = defaultReplaceFileReferencesRequest(job2);

        // When we send them as a batch
        batcher().sendBatch(List.of(
                commitRequest("test-table", request1),
                commitRequest("test-table", request2)));

        // Then they are combined into one transaction
        assertThat(queue).containsExactly(
                StateStoreCommitRequest.create("test-table",
                        new ReplaceFileReferencesTransaction(List.of(request1, request2))));
    }

    // Further cases are tested here...

    private CompactionCommitBatcher batcher() {
        // The method reference `queue::add` implements the interface StateStoreCommitRequestSender,
        // which is also implemented by SqsFifoStateStoreCommitRequestSender in the tests shown below.
        return new CompactionCommitBatcher(queue::add);
    }

}

public class SqsFifoStateStoreCommitRequestSenderIT extends LocalStackTestBase {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final TableProperties tableProperties = createTestTableProperties(instanceProperties, createSchemaWithKey("key"));

    @BeforeEach
    void setup() {
        instanceProperties.set(STATESTORE_COMMITTER_QUEUE_URL, createFifoQueueGetUrl());
        tableProperties.set(TABLE_ID, "test-table");
    }

    @Test
    void shouldSendCommitToSqs() {
        // Given
        CompactionJob job = jobFactory(tableProperties).createCompactionJobWithFilenames(
                "test-job", List.of("test.parquet"), "root");
        FileReferenceTransaction transaction = new ReplaceFileReferencesTransaction(
            List.of(defaultReplaceFileReferencesRequest(job)));

        // When
        sender().send(StateStoreCommitRequest.create("test-table", transaction));

        // Then
        assertThat(receiveCommitRequests())
                .containsExactly(StateStoreCommitRequest.create("test-table", transaction));
    }

    // Note that it is not necessary to test different transaction types here as serialisation/deserialisation
    // is tested separately in in-memory unit tests. The createSerDe method used below creates an object to serialise
    // and deserialise to/from JSON, and it is tested elsewhere.

    private StateStoreCommitRequestSender sender() {
        return new SqsFifoStateStoreCommitRequestSender(instanceProperties, createSerDe(), sqsClient);
    }

    private List<StateStoreCommitRequest> receiveCommitRequests() {
        return receiveMessages(instanceProperties.get(STATESTORE_COMMITTER_QUEUE_URL))
                .map(createSerDe()::fromJson)
                .toList();
    }

}
```

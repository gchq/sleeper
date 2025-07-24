Test Strategy
=============

The Java code includes unit tests, integration tests and system tests. We use JUnit 5, with AssertJ for assertions.

The Rust code includes unit tests and integration tests. We use the built-in test framework.

We also have a manual testing setup that combines system test tools with a deployed instance of Sleeper, documented in
the [system tests guide](system-tests.md#manual-testing).

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
  - Tests should be added as soon as possible

We also write system tests for all features that are not experimental. We keep the number of system tests per feature
low, as these tests are relatively slow to run. They should verify that each feature is functional when deployed in AWS,
and test properties such as performance and throughput. When we add a new feature, we add one or two simple cases to
this test suite, as a complement to more detailed unit testing.

### Design techniques

We use tests to define the behaviour of the system, writing all tests in the style of behaviour driven development
(BDD). The following article by Dan North defines this term, though not all examples match our style and conventions:
https://dannorth.net/introducing-bdd/

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

We avoid mocking wherever possible, and prefer to use test fakes, e.g. implement an interface to a database with a
wrapper around an in-memory collection, e.g. HashMap or ArrayList.

We use test helper methods to make tests as readable as possible, and as close as possible to a set of English
given/when/then statements.

For example, when writing code to send a message to SQS, we would write an interface with a method to send a message. We
can then implement that as a method reference to the `add` method on a `LinkedList`, and use that to test the logic.

That lets us minimise the tests that need to run against SQS, which we can do separately against LocalStack. We do the
same thing for S3 and DynamoDB, but with more complex in-memory fakes that implement an interface with a class rather
than a method reference.

Below are some simplified examples based on real Sleeper code. In these examples, we send requests to commit a
transaction to update a Sleeper table. A compaction removes some files from a Sleeper table and replaces them with its
output file. We combine the results of multiple compactions into one transaction, and send the transaction to an
SQS queue to be committed to the state store.

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

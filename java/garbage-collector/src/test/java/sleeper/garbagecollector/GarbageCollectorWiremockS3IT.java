/*
 * Copyright 2022-2026 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package sleeper.garbagecollector;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.S3Exception;

import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToXml;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.table.TableProperty.GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION;
import static sleeper.core.statestore.FilesReportTestHelper.referencedAndUnreferencedFilesReport;
import static sleeper.core.statestore.FilesReportTestHelper.referencedFilesReport;
import static sleeper.core.util.ThreadSleepTestHelper.recordWaits;
import static sleeper.localstack.test.WiremockAwsV2ClientHelper.wiremockAwsV2ClientWithRetryAttempts;

@WireMockTest
class GarbageCollectorWiremockS3IT extends GarbageCollectorTestBase {

    private static final String DELETE_URL = "/test-bucket?delete";
    private static final String PARTITION_PATH = "test-table/data/partition_root/";

    S3Client s3Client;
    TableProperties table = createTableWithId("test-table");
    StateStore stateStore = stateStore(table);
    List<Duration> threadSleeps = new ArrayList<>();

    @BeforeEach
    void setUp(WireMockRuntimeInfo runtimeInfo) {
        s3Client = wiremockAwsV2ClientWithRetryAttempts(1, runtimeInfo,
                S3Client.builder()
                        .serviceConfiguration(S3Configuration.builder()
                                .pathStyleAccessEnabled(true)
                                .build()));

        instanceProperties.set(DATA_BUCKET, "test-bucket");
    }

    @Test
    void shouldTriggerRateLimitExceptionAndReduceRate() throws Exception {
        // Given
        Instant currentTime = Instant.parse("2023-06-28T13:46:00Z");
        Instant oldEnoughTime = currentTime.minus(Duration.ofMinutes(11));
        stateStore.fixFileUpdateTime(oldEnoughTime);
        table.setNumber(GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION, 10);
        createFileWithNoReferencesByCompaction(stateStore, "old-file", "new-file");
        stubFor(post(DELETE_URL)
                .inScenario("retry")
                .whenScenarioStateIs(Scenario.STARTED)
                .willSetStateTo("retry")
                .willReturn(aResponse()
                        .withStatus(503)
                        .withBody(errorResponse("SlowDown", "Reduce your request rate."))));
        stubFor(post(DELETE_URL)
                .inScenario("retry")
                .whenScenarioStateIs("retry")
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBody(deleteResult(
                                deleted("old-file.parquet"),
                                deleted("old-file.sketches")))));

        // When
        collectGarbageAtTime(currentTime);

        // Then
        verify(2, postRequestedFor(urlEqualTo(DELETE_URL))
                .withRequestBody(equalToXml(deleteRequest("old-file.parquet", "old-file.sketches"))));
        assertThat(stateStore.getAllFilesWithMaxUnreferenced(10))
                .isEqualTo(referencedFilesReport(oldEnoughTime, fileReference("new-file")));
        assertThat(threadSleeps).hasSize(1);
    }

    @Test
    void shouldNotAttemptRetryOnNonSlowDownS3Exception() throws Exception {
        // Given
        Instant currentTime = Instant.parse("2023-06-28T13:46:00Z");
        Instant oldEnoughTime = currentTime.minus(Duration.ofMinutes(11));
        stateStore.fixFileUpdateTime(oldEnoughTime);
        table.setNumber(GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION, 10);
        createFileWithNoReferencesByCompaction(stateStore, "old-file", "new-file");
        stubFor(post(DELETE_URL)
                .willReturn(aResponse()
                        .withStatus(503)
                        .withBody(errorResponse("ServiceUnavailable", "Service is unable to handle request."))));

        // When / Then
        assertThatThrownBy(() -> collectGarbageAtTime(currentTime))
                .isInstanceOf(FailedGarbageCollectionException.class)
                .hasCauseInstanceOf(S3Exception.class);
        verify(1, postRequestedFor(urlEqualTo(DELETE_URL))
                .withRequestBody(equalToXml(deleteRequest("old-file.parquet", "old-file.sketches"))));
        assertThat(stateStore.getAllFilesWithMaxUnreferenced(10)).isEqualTo(
                referencedAndUnreferencedFilesReport(oldEnoughTime,
                        List.of(fileReference("new-file")),
                        List.of(filename("old-file"))));
        assertThat(threadSleeps).isEmpty();
    }

    @Test
    void shouldRecordOneFailureForAllFilesInFailedBatch() throws Exception {
        // Given
        Instant currentTime = Instant.parse("2023-06-28T13:46:00Z");
        Instant oldEnoughTime = currentTime.minus(Duration.ofMinutes(11));
        stateStore.fixFileUpdateTime(oldEnoughTime);
        table.setNumber(GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION, 10);
        createFileWithNoReferencesByCompaction(stateStore, "old-file-1", "new-file-1");
        createFileWithNoReferencesByCompaction(stateStore, "old-file-2", "new-file-2");
        stubFor(post(DELETE_URL)
                .willReturn(aResponse()
                        .withStatus(503)
                        .withBody(errorResponse("ServiceUnavailable", "Service is unable to handle request."))));

        // When / Then
        assertThatThrownBy(() -> collectGarbageAtTime(currentTime))
                .isInstanceOfSatisfying(FailedGarbageCollectionException.class,
                        e -> assertThat(e.getTableFailures()).singleElement().satisfies(failure -> {
                            assertThat(failure.fileFailures()).singleElement().satisfies(fileFailure -> {
                                assertThat(fileFailure.filenames())
                                        .containsExactlyInAnyOrder(filename("old-file-1"), filename("old-file-2"));
                                assertThat(fileFailure.cause()).isInstanceOf(S3Exception.class);
                            });
                        }));
        verify(1, postRequestedFor(urlEqualTo(DELETE_URL)));
        assertThat(stateStore.getAllFilesWithMaxUnreferenced(10)).isEqualTo(
                referencedAndUnreferencedFilesReport(oldEnoughTime,
                        List.of(fileReference("new-file-1"), fileReference("new-file-2")),
                        List.of(filename("old-file-1"), filename("old-file-2"))));
        assertThat(threadSleeps).isEmpty();
    }

    @Test
    void shouldNotRecordFileAsDeletedWhenOnlySomeOfItsObjectsWereDeleted() throws Exception {
        // Given
        Instant currentTime = Instant.parse("2023-06-28T13:46:00Z");
        Instant oldEnoughTime = currentTime.minus(Duration.ofMinutes(11));
        stateStore.fixFileUpdateTime(oldEnoughTime);
        table.setNumber(GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION, 10);
        createFileWithNoReferencesByCompaction(stateStore, "old-file", "new-file");
        stubFor(post(DELETE_URL)
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBody(deleteResult(
                                deleted("old-file.parquet"),
                                failedToDelete("old-file.sketches")))));

        // When / Then
        assertThatThrownBy(() -> collectGarbageAtTime(currentTime))
                .isInstanceOf(FailedGarbageCollectionException.class)
                .cause().hasMessage("Failed to delete object " + PARTITION_PATH + "old-file.sketches: " +
                        "InternalError - We encountered an internal error. Please try again.");
        verify(1, postRequestedFor(urlEqualTo(DELETE_URL))
                .withRequestBody(equalToXml(deleteRequest("old-file.parquet", "old-file.sketches"))));
        assertThat(stateStore.getAllFilesWithMaxUnreferenced(10)).isEqualTo(
                referencedAndUnreferencedFilesReport(oldEnoughTime,
                        List.of(fileReference("new-file")),
                        List.of(filename("old-file"))));
        assertThat(threadSleeps).isEmpty();
    }

    @Test
    void shouldDeletePartiallyDeletedFileOnNextRun() throws Exception {
        // Given
        Instant currentTime = Instant.parse("2023-06-28T13:46:00Z");
        Instant oldEnoughTime = currentTime.minus(Duration.ofMinutes(11));
        stateStore.fixFileUpdateTime(oldEnoughTime);
        table.setNumber(GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION, 10);
        createFileWithNoReferencesByCompaction(stateStore, "old-file", "new-file");
        stubFor(post(DELETE_URL)
                .inScenario("retry")
                .whenScenarioStateIs(Scenario.STARTED)
                .willSetStateTo("retry")
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBody(deleteResult(
                                deleted("old-file.parquet"),
                                failedToDelete("old-file.sketches")))));
        stubFor(post(DELETE_URL)
                .inScenario("retry")
                .whenScenarioStateIs("retry")
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBody(deleteResult(
                                deleted("old-file.parquet"),
                                deleted("old-file.sketches")))));

        // When
        assertThatThrownBy(() -> collectGarbageAtTime(currentTime))
                .isInstanceOf(FailedGarbageCollectionException.class);
        int deletedOnRetry = collectGarbageAtTime(currentTime);

        // Then
        assertThat(deletedOnRetry).isOne();
        verify(2, postRequestedFor(urlEqualTo(DELETE_URL))
                .withRequestBody(equalToXml(deleteRequest("old-file.parquet", "old-file.sketches"))));
        assertThat(stateStore.getAllFilesWithMaxUnreferenced(10))
                .isEqualTo(referencedFilesReport(oldEnoughTime, fileReference("new-file")));
        assertThat(threadSleeps).isEmpty();
    }

    @Test
    void shouldRecordFullyDeletedFileWhenAnotherFileInBatchWasOnlyPartiallyDeleted() throws Exception {
        // Given
        Instant currentTime = Instant.parse("2023-06-28T13:46:00Z");
        Instant oldEnoughTime = currentTime.minus(Duration.ofMinutes(11));
        stateStore.fixFileUpdateTime(oldEnoughTime);
        table.setNumber(GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION, 10);
        createFileWithNoReferencesByCompaction(stateStore, "old-file-1", "new-file-1");
        createFileWithNoReferencesByCompaction(stateStore, "old-file-2", "new-file-2");
        stubFor(post(DELETE_URL)
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBody(deleteResult(
                                deleted("old-file-1.parquet"),
                                deleted("old-file-1.sketches"),
                                deleted("old-file-2.parquet"),
                                failedToDelete("old-file-2.sketches")))));

        // When / Then
        assertThatThrownBy(() -> collectGarbageAtTime(currentTime))
                .isInstanceOf(FailedGarbageCollectionException.class)
                .cause().hasMessage("Failed to delete object " + PARTITION_PATH + "old-file-2.sketches: " +
                        "InternalError - We encountered an internal error. Please try again.");
        verify(1, postRequestedFor(urlEqualTo(DELETE_URL)));
        assertThat(stateStore.getAllFilesWithMaxUnreferenced(10)).isEqualTo(
                referencedAndUnreferencedFilesReport(oldEnoughTime,
                        List.of(fileReference("new-file-1"), fileReference("new-file-2")),
                        List.of(filename("old-file-2"))));
        assertThat(threadSleeps).isEmpty();
    }

    @Test
    void shouldNotRecordFileAsDeletedWhenItsObjectsWereSplitAcrossBatchesAndOneFailed() throws Exception {
        // Given
        Instant currentTime = Instant.parse("2023-06-28T13:46:00Z");
        Instant oldEnoughTime = currentTime.minus(Duration.ofMinutes(11));
        stateStore.fixFileUpdateTime(oldEnoughTime);
        table.setNumber(GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION, 10);
        createFileWithNoReferencesByCompaction(stateStore, "old-file", "new-file");
        stubFor(post(DELETE_URL)
                .withRequestBody(equalToXml(deleteRequest("old-file.parquet")))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBody(deleteResult(
                                deleted("old-file.parquet")))));
        stubFor(post(DELETE_URL)
                .withRequestBody(equalToXml(deleteRequest("old-file.sketches")))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBody(deleteResult(
                                failedToDelete("old-file.sketches")))));

        // When / Then
        assertThatThrownBy(() -> collectGarbageAtTimeWithS3BatchSize(currentTime, 1))
                .isInstanceOf(FailedGarbageCollectionException.class);
        verify(2, postRequestedFor(urlEqualTo(DELETE_URL)));
        assertThat(stateStore.getAllFilesWithMaxUnreferenced(10)).isEqualTo(
                referencedAndUnreferencedFilesReport(oldEnoughTime,
                        List.of(fileReference("new-file")),
                        List.of(filename("old-file"))));
        assertThat(threadSleeps).isEmpty();
    }

    protected FileReferenceFactory fileReferenceFactory() {
        return FileReferenceFactory.from(instanceProperties, table, partitions);
    }

    protected int collectGarbageAtTime(Instant time) throws Exception {
        return collectGarbageAtTimeWithS3BatchSize(time, 10);
    }

    private int collectGarbageAtTimeWithS3BatchSize(Instant time, int s3BatchSize) throws Exception {
        return collectorWithDeleteAction(new S3DeleteFiles(s3Client, s3BatchSize, recordWaits(threadSleeps)))
                .runAtTime(time, tables);
    }

    private String filename(String filePath) {
        return fileReference(filePath).getFilename();
    }

    private static String deleteRequest(String... objectNames) {
        return "<Delete>" + Stream.of(objectNames)
                .map(objectName -> "<Object>" + key(objectName) + "</Object>")
                .collect(joining()) + "</Delete>";
    }

    private static String deleteResult(String... results) {
        return "<DeleteResult>" + String.join("", results) + "</DeleteResult>";
    }

    private static String deleted(String objectName) {
        return "<Deleted>" + key(objectName) + "</Deleted>";
    }

    private static String failedToDelete(String objectName) {
        return "<Error>" + key(objectName) +
                "<Code>InternalError</Code><Message>We encountered an internal error. Please try again.</Message></Error>";
    }

    private static String errorResponse(String code, String message) {
        return "<Error><Code>" + code + "</Code><Message>" + message + "</Message></Error>";
    }

    private static String key(String objectName) {
        return "<Key>" + PARTITION_PATH + objectName + "</Key>";
    }
}

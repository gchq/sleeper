/*
 * Copyright 2022-2025 Crown Copyright
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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.core.statestore.StateStore;
import sleeper.localstack.test.WiremockAwsV1ClientHelper;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.DATA_BUCKET;
import static sleeper.core.properties.table.TableProperty.GARBAGE_COLLECTOR_DELAY_BEFORE_DELETION;
import static sleeper.core.statestore.FilesReportTestHelper.activeAndReadyForGCFilesReport;
import static sleeper.core.statestore.FilesReportTestHelper.activeFilesReport;
import static sleeper.core.util.ThreadSleepTestHelper.recordWaits;

@WireMockTest
class GarbageCollectorWiremockS3IT extends GarbageCollectorTestBase {

    AmazonS3 s3Client;
    TableProperties table = createTableWithId("test-table");
    StateStore stateStore = stateStore(table);
    List<Duration> threadSleeps = new ArrayList<>();

    @BeforeEach
    void setUp(WireMockRuntimeInfo runtimeInfo) {
        s3Client = WiremockAwsV1ClientHelper.buildAwsV1Client(runtimeInfo,
                AmazonS3ClientBuilder.standard()
                        .withPathStyleAccessEnabled(true)
                        .withClientConfiguration(new ClientConfiguration()
                                .withMaxErrorRetry(0)));
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
        stubFor(post("/test-bucket/?delete")
                .inScenario("retry")
                .whenScenarioStateIs(Scenario.STARTED)
                .willSetStateTo("retry")
                .willReturn(aResponse()
                        .withStatus(503)
                        .withBody("<Error><Code>SlowDown</Code><Message>Reduce your request rate.</Message></Error>")));
        stubFor(post("/test-bucket/?delete")
                .inScenario("retry")
                .whenScenarioStateIs("retry")
                .willReturn(aResponse()
                        .withStatus(200)
                        .withBody("<DeleteResult><Deleted><Key>test-table/data/partition_root/old-file.parquet</Key></Deleted>" +
                                "<Deleted><Key>test-table/data/partition_root/old-file.sketches</Key></Deleted></DeleteResult>")));

        // When
        collectGarbageAtTime(currentTime);

        // Then
        verify(2, postRequestedFor(urlEqualTo("/test-bucket/?delete"))
                .withRequestBody(equalTo(
                        "<Delete><Object><Key>test-table/data/partition_root/old-file.parquet</Key></Object>" +
                                "<Object><Key>test-table/data/partition_root/old-file.sketches</Key></Object></Delete>")));
        assertThat(stateStore.getAllFilesWithMaxUnreferenced(10))
                .isEqualTo(activeFilesReport(oldEnoughTime, activeReference("new-file")));
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
        stubFor(post("/test-bucket/?delete")
                .willReturn(aResponse()
                        .withStatus(503)
                        .withBody("<Error><Code>ServiceUnavailable</Code><Message>Service is unable to handle request.</Message></Error>")));

        // When / Then
        assertThatThrownBy(() -> collectGarbageAtTime(currentTime))
                .isInstanceOf(FailedGarbageCollectionException.class)
                .hasCauseInstanceOf(AmazonS3Exception.class);
        verify(1, postRequestedFor(urlEqualTo("/test-bucket/?delete"))
                .withRequestBody(equalTo(
                        "<Delete><Object><Key>test-table/data/partition_root/old-file.parquet</Key></Object>" +
                                "<Object><Key>test-table/data/partition_root/old-file.sketches</Key></Object></Delete>")));
        assertThat(stateStore.getAllFilesWithMaxUnreferenced(10)).isEqualTo(
                activeAndReadyForGCFilesReport(oldEnoughTime,
                        List.of(activeReference("new-file")),
                        List.of("s3a://test-bucket/test-table/data/partition_root/old-file.parquet")));
        assertThat(threadSleeps).isEmpty();
    }

    protected FileReferenceFactory fileReferenceFactory() {
        return FileReferenceFactory.from(instanceProperties, table, partitions);
    }

    protected int collectGarbageAtTime(Instant time) throws Exception {
        return collectorWithDeleteAction(new S3DeleteFiles(s3Client, 10, recordWaits(threadSleeps)))
                .runAtTime(time, tables);
    }
}

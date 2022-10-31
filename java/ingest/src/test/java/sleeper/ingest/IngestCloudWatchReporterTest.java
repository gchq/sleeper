/*
 * Copyright 2022 Crown Copyright
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

package sleeper.ingest;

import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import sleeper.ingest.job.AWSCloudWatchReporter;
import sleeper.ingest.job.CloudWatchReporter;
import sleeper.ingest.job.FixedCloudWatchReporter;
import sleeper.ingest.testutils.AwsExternalResource;

import static org.assertj.core.api.Assertions.assertThat;

public class IngestCloudWatchReporterTest {
    @ClassRule
    public static final AwsExternalResource AWS_EXTERNAL_RESOURCE = new AwsExternalResource(
            LocalStackContainer.Service.CLOUDWATCH);


    @Test
    public void shouldReportMetricsInAWS() {
        // Given/When
        CloudWatchReporter reporter = AWSCloudWatchReporter.from(AWS_EXTERNAL_RESOURCE.getCloudWatchClient());
        boolean result = reporter.report(createTestMetricRequest());
        // Then
        assertThat(result).isTrue();
    }

    @Test
    public void shouldReportMetricsInFixedReporter() {
        // Given/When
        CloudWatchReporter reporter = FixedCloudWatchReporter.create();
        boolean result = reporter.report(createTestMetricRequest());
        // Then
        assertThat(result).isTrue();
    }

    private PutMetricDataRequest createTestMetricRequest() {
        return new PutMetricDataRequest().withMetricData(new MetricDatum()
                .withMetricName("TestMetric")
                .withValue((double) 10)
                .withUnit(StandardUnit.Count)
                .withDimensions(
                        new Dimension().withName("instanceId").withValue("test-instance"),
                        new Dimension().withName("tableName").withValue("test-table")
                ));
    }
}

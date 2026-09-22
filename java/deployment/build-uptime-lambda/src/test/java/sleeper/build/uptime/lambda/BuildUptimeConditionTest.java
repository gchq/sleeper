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
package sleeper.build.uptime.lambda;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.build.uptime.lambda.BuildUptimeCondition.TEST_FINISHED_FROM_TODAY;

public class BuildUptimeConditionTest {

    private final Map<String, String> s3PathToObject = new HashMap<>();

    @Test
    void shouldNotFindTestFinishedFromTodayWhenNoSummaryIsInBucket() {
        // When / Then
        assertThat(checkTestFinishedFromTodayInBucketAtTime("test-bucket", Instant.now()))
                .isFalse();
    }

    @Test
    void shouldNotFindTestFinishedFromTodayWhenLastRunIsMoreThan12HoursOld() {
        // Given
        putSummaryJsonInBucket("test-bucket", "{" +
                "\"executions\": [{" +
                "\"startTime\": \"2026-09-20T20:00:00Z\"" +
                "}]}");
        Instant timeNow = Instant.parse("2026-09-21T08:00:01Z");

        // When / Then
        assertThat(checkTestFinishedFromTodayInBucketAtTime("test-bucket", timeNow))
                .isFalse();
    }

    @Test
    void shouldFindTestFinishedFromTodayWhenLastRunIsLessThan12HoursOld() {
        // Given
        putSummaryJsonInBucket("test-bucket", "{" +
                "\"executions\": [{" +
                "\"startTime\": \"2026-09-20T20:00:00Z\"" +
                "}]}");
        Instant timeNow = Instant.parse("2026-09-21T07:59:59Z");

        // When / Then
        assertThat(checkTestFinishedFromTodayInBucketAtTime("test-bucket", timeNow))
                .isTrue();
    }

    private boolean checkTestFinishedFromTodayInBucketAtTime(String bucket, Instant timeNow) {
        return BuildUptimeCondition.conditionAndBucket(TEST_FINISHED_FROM_TODAY, bucket)
                .check(getS3ObjectAsString(), timeNow);
    }

    private void putSummaryJsonInBucket(String bucket, String summaryJson) {
        s3PathToObject.put(bucket + "/summary.json", summaryJson);
    }

    private GetS3ObjectAsString getS3ObjectAsString() {
        return (bucket, key) -> Optional.ofNullable(s3PathToObject.get(bucket + "/" + key));
    }

}

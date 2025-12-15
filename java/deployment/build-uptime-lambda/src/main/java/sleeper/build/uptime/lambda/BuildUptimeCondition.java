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
package sleeper.build.uptime.lambda;

import software.amazon.awssdk.services.s3.S3Client;

import java.time.Instant;

public class BuildUptimeCondition {

    public static final String TEST_FINISHED_FROM_TODAY = "testFinishedFromToday";

    private final String condition;
    private final String testBucket;

    private BuildUptimeCondition(String condition, String testBucket) {
        this.condition = condition;
        this.testBucket = testBucket;
    }

    public static BuildUptimeCondition of(BuildUptimeEvent event) {
        return new BuildUptimeCondition(event.getCondition(), event.getTestBucket());
    }

    public boolean check(S3Client s3, Instant now) {
        return check(GetS3ObjectAsString.fromClient(s3), now);
    }

    public boolean check(GetS3ObjectAsString s3, Instant now) {
        if (condition == null) {
            return true;
        }
        switch (condition) {
            case TEST_FINISHED_FROM_TODAY:
                NightlyTestSummaryTable summary = NightlyTestSummaryTable.fromS3(s3, testBucket);
                return summary.containsTestFromToday(now);
            default:
                return false;
        }
    }

}

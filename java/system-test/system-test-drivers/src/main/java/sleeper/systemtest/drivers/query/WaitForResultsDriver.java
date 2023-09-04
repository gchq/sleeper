/*
 * Copyright 2022-2023 Crown Copyright
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

package sleeper.systemtest.drivers.query;

import com.amazonaws.services.s3.AmazonS3;

import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.drivers.instance.SleeperInstanceContext;

import java.time.Duration;
import java.util.List;

import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.QUERY_RESULTS_BUCKET;

public class WaitForResultsDriver {
    private final SleeperInstanceContext instance;
    private final AmazonS3 s3;
    private final PollWithRetries poll = PollWithRetries.intervalAndPollingTimeout(
            Duration.ofSeconds(10), Duration.ofMinutes(5L));

    public WaitForResultsDriver(SleeperInstanceContext instance, AmazonS3 s3) {
        this.instance = instance;
        this.s3 = s3;
    }

    public void waitForResults(List<String> queryIds) throws InterruptedException {
        poll.pollUntil("query finished", () -> queryIds.stream().allMatch(this::queryFinished));
    }

    private boolean queryFinished(String queryId) {
        return !s3.listObjects(instance.getInstanceProperties().get(QUERY_RESULTS_BUCKET), queryId)
                .getObjectSummaries().isEmpty();
    }
}

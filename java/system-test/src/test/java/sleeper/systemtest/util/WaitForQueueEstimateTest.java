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
package sleeper.systemtest.util;

import org.junit.jupiter.api.Test;

import sleeper.clients.util.PollWithRetries;
import sleeper.configuration.properties.InstanceProperties;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.COMPACTION_JOB_QUEUE_URL;
import static sleeper.job.common.QueueMessageCountsInMemory.singleQueueVisibleMessages;

class WaitForQueueEstimateTest {

    @Test
    void shouldTimeOutWaitingForNonEmptyQueue() {
        // Given
        InstanceProperties properties = new InstanceProperties();
        properties.set(COMPACTION_JOB_QUEUE_URL, "test-job-queue");
        WaitForQueueEstimate wait = WaitForQueueEstimate.notEmpty(
                singleQueueVisibleMessages("test-job-queue", 0),
                properties, COMPACTION_JOB_QUEUE_URL);
        PollWithRetries poll = PollWithRetries.intervalAndMaxPolls(0, 1);

        // When / Then
        assertThatThrownBy(() -> wait.pollUntilFinished(poll))
                .isInstanceOf(PollWithRetries.TimedOutException.class);
    }

    @Test
    void shouldFinishWaitingForNonEmptyQueue() {
        // Given
        InstanceProperties properties = new InstanceProperties();
        properties.set(COMPACTION_JOB_QUEUE_URL, "test-job-queue");
        WaitForQueueEstimate wait = WaitForQueueEstimate.notEmpty(
                singleQueueVisibleMessages("test-job-queue", 1),
                properties, COMPACTION_JOB_QUEUE_URL);
        PollWithRetries poll = PollWithRetries.intervalAndMaxPolls(0, 1);

        // When / Then
        assertThatCode(() -> wait.pollUntilFinished(poll))
                .doesNotThrowAnyException();
    }
}

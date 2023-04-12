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

package sleeper.status.report.ingest.job;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.job.common.QueueMessageCount;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;

public class IngestQueueMessages {
    private final int ingestJobMessageCount;

    private IngestQueueMessages(int ingestJobMessageCount) {
        this.ingestJobMessageCount = ingestJobMessageCount;
    }

    public static IngestQueueMessages from(InstanceProperties properties, QueueMessageCount.Client client) {
        int ingestJobMessageCount = client.getQueueMessageCount(properties.get(INGEST_JOB_QUEUE_URL))
                .getApproximateNumberOfMessages();
        return new IngestQueueMessages(ingestJobMessageCount);
    }

    public int getIngestJobMessageCount() {
        return ingestJobMessageCount;
    }
}

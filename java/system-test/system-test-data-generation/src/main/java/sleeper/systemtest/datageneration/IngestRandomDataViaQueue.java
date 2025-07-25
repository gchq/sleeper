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
package sleeper.systemtest.datageneration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.ingest.core.job.IngestJob;
import sleeper.ingest.core.job.IngestJobSerDe;
import sleeper.systemtest.configuration.SystemTestDataGenerationJob;

import java.util.Collections;

import static sleeper.core.properties.table.TableProperty.TABLE_NAME;

public class IngestRandomDataViaQueue {
    private static final Logger LOGGER = LoggerFactory.getLogger(IngestRandomDataViaQueue.class);

    private IngestRandomDataViaQueue() {
    }

    public static void sendJob(
            String jobId, String dir, SystemTestDataGenerationJob job, InstanceIngestSession session) {

        IngestJob ingestJob = IngestJob.builder()
                .tableName(session.tableProperties().get(TABLE_NAME))
                .id(jobId)
                .files(Collections.singletonList(dir))
                .build();
        String jsonJob = new IngestJobSerDe().toJson(ingestJob);
        String queueUrl = job.getIngestQueue().getJobQueueUrl(session.instanceProperties());
        LOGGER.debug("Sending message to ingest queue {}: {}", queueUrl, jsonJob);
        session.sqs().sendMessage(send -> send
                .queueUrl(queueUrl)
                .messageBody(jsonJob));
    }
}

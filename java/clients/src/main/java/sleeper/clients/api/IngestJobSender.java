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
package sleeper.clients.api;

import software.amazon.awssdk.services.sqs.SqsClient;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.ingest.core.job.IngestJob;
import sleeper.ingest.core.job.IngestJobSerDe;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;

/**
 * Sends an ingest job to a Sleeper instance. Note that at scale bulk import should be used instead.
 */
@FunctionalInterface
public interface IngestJobSender {

    /**
     * Sends an ingest job to add data to a Sleeper table.
     *
     * @param job the job
     */
    void sendFilesToIngest(IngestJob job);

    /**
     * Creates an object to send ingest jobs to an Amazon SQS queue.
     *
     * @param  instanceProperties the instance properties
     * @param  sqsClient          the SQS client
     * @return                    the sender
     */
    static IngestJobSender toSqs(InstanceProperties instanceProperties, SqsClient sqsClient) {
        IngestJobSerDe serDe = new IngestJobSerDe();
        return job -> sqsClient.sendMessage(send -> send
                .queueUrl(instanceProperties.get(INGEST_JOB_QUEUE_URL))
                .messageBody(serDe.toJson(job)));
    }
}

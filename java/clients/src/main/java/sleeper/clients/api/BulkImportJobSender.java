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

import sleeper.bulkimport.core.configuration.BulkImportPlatform;
import sleeper.bulkimport.core.job.BulkImportJob;
import sleeper.bulkimport.core.job.BulkImportJobSerDe;
import sleeper.core.properties.instance.InstanceProperties;

/**
 * Sends a bulk import job to a Sleeper instance.
 */
@FunctionalInterface
public interface BulkImportJobSender {

    /**
     * Sends a bulk import job to add data to a Sleeper table.
     *
     * @param platform the platform to run the job on
     * @param job      the job
     */
    void sendFilesToBulkImport(BulkImportPlatform platform, BulkImportJob job);

    /**
     * Creates an object to send bulk import jobs to an Amazon SQS queue for the given platform.
     *
     * @param  instanceProperties the instance properties
     * @param  sqsClient          the SQS client
     * @return                    the sender
     */
    static BulkImportJobSender toSqs(InstanceProperties instanceProperties, SqsClient sqsClient) {
        BulkImportJobSerDe serDe = new BulkImportJobSerDe();
        return (platform, job) -> sqsClient.sendMessage(send -> send
                .queueUrl(platform.getBulkImportQueueUrl(instanceProperties))
                .messageBody(serDe.toJson(job)));
    }
}

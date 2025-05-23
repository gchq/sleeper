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
package sleeper.clients.api;

import software.amazon.awssdk.services.sqs.SqsClient;

import sleeper.bulkimport.core.configuration.BulkImportPlatform;
import sleeper.bulkimport.core.job.BulkImportJob;
import sleeper.bulkimport.core.job.BulkImportJobSerDe;
import sleeper.core.properties.instance.InstanceProperties;

@FunctionalInterface
public interface BulkImportJobSender {

    void sendFilesToBulkImport(BulkImportPlatform platform, BulkImportJob job);

    static BulkImportJobSender toSqs(InstanceProperties instanceProperties, SqsClient sqsClient) {
        BulkImportJobSerDe serDe = new BulkImportJobSerDe();
        return (platform, job) -> sqsClient.sendMessage(send -> send
                .queueUrl(platform.getBulkImportQueueUrl(instanceProperties))
                .messageBody(serDe.toJson(job)));
    }
}

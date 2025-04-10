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

import com.amazonaws.services.sqs.AmazonSQS;

import sleeper.bulkimport.core.configuration.BulkImportPlatform;
import sleeper.bulkimport.core.job.BulkImportJob;
import sleeper.bulkimport.core.job.BulkImportJobSerDe;
import sleeper.core.properties.instance.InstanceProperties;

@FunctionalInterface
public interface SleeperClientImport {
    default void bulkImportFilesFromS3(String platform, BulkImportJob job) {
        bulkImportFilesFromS3(BulkImportPlatform.fromString(platform), job);
    }

    void bulkImportFilesFromS3(BulkImportPlatform platform, BulkImportJob job);

    static SleeperClientImport bulkImportParquetFilesFromS3(InstanceProperties instanceProperties, AmazonSQS sqsClient) {
        return (platform, job) -> {
            sqsClient.sendMessage(platform.getBulkImportQueueUrl(instanceProperties), new BulkImportJobSerDe().toJson((job)));
        };
    }
}

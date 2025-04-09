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

import sleeper.bulkimport.core.job.BulkImportJob;
import sleeper.bulkimport.core.job.BulkImportJobSerDe;
import sleeper.core.properties.instance.InstanceProperties;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EKS_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EMR_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_EMR_SERVERLESS_JOB_QUEUE_URL;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_PERSISTENT_EMR_JOB_QUEUE_URL;

@FunctionalInterface
public interface SleeperClientImport {
    void bulkImportFilesFromS3(InstanceProperties properties, String platform, BulkImportJob job);

    static SleeperClientImport bulkImportParquetFilesFromS3(AmazonSQS sqsClient) {
        return (properties, platform, job) -> {
            sqsClient.sendMessage(determinePlatform(properties, platform), new BulkImportJobSerDe().toJson((job)));
        };
    }

    static String determinePlatform(InstanceProperties instanceProperties, String platform) {
        if (platform.equalsIgnoreCase("EMR")) {
            return instanceProperties.get(BULK_IMPORT_EMR_JOB_QUEUE_URL);
        } else if (platform.equalsIgnoreCase("EKS")) {
            return instanceProperties.get(BULK_IMPORT_EKS_JOB_QUEUE_URL);
        } else if (platform.equalsIgnoreCase("PersistentEMR")) {
            return instanceProperties.get(BULK_IMPORT_PERSISTENT_EMR_JOB_QUEUE_URL);
        } else if (platform.equalsIgnoreCase("EMRServerless")) {
            return instanceProperties.get(BULK_IMPORT_EMR_SERVERLESS_JOB_QUEUE_URL);
        } else {
            throw new RuntimeException("Platform must be 'EMR' or 'PersistentEMR' or 'EKS' or 'EMRServerless'");
        }
    }
}

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

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.ingest.core.job.IngestJob;
import sleeper.ingest.core.job.IngestJobSerDe;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.INGEST_JOB_QUEUE_URL;

@FunctionalInterface
public interface IngestJobSender {
    void sendFilesToIngest(IngestJob job);

    static IngestJobSender ingestParquetFilesFromS3(InstanceProperties instanceProperties, AmazonSQS sqsClient) {
        IngestJobSerDe serDe = new IngestJobSerDe();
        return (job) -> {
            sqsClient.sendMessage(instanceProperties.get(INGEST_JOB_QUEUE_URL), serDe.toJson(job));
        };
    }
}

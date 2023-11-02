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

package sleeper.systemtest.drivers.ingest;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.s3.AmazonS3;
import software.amazon.awssdk.services.emrserverless.EmrServerlessClient;

import sleeper.bulkimport.job.BulkImportJob;
import sleeper.bulkimport.job.BulkImportJobSerDe;
import sleeper.bulkimport.starter.executor.BulkImportArguments;
import sleeper.bulkimport.starter.executor.EmrServerlessPlatformExecutor;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.ingest.status.store.job.IngestJobStatusStoreFactory;
import sleeper.systemtest.drivers.instance.SleeperInstanceContext;

import java.time.Instant;
import java.util.UUID;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_BUCKET;
import static sleeper.ingest.job.status.IngestJobValidatedEvent.ingestJobAccepted;

public class DirectEmrServerlessDriver {
    private final InstanceProperties properties;
    private final IngestJobStatusStore jobStatusStore;
    private final EmrServerlessPlatformExecutor executor;
    private final AmazonS3 s3Client;

    public DirectEmrServerlessDriver(SleeperInstanceContext instance,
                                     AmazonS3 s3Client, AmazonDynamoDB dynamoDBClient, EmrServerlessClient emrClient) {
        this(instance.getInstanceProperties(),
                IngestJobStatusStoreFactory.getStatusStore(dynamoDBClient, instance.getInstanceProperties()),
                new EmrServerlessPlatformExecutor(emrClient, instance.getInstanceProperties()), s3Client);
    }

    public DirectEmrServerlessDriver(InstanceProperties properties,
                                     IngestJobStatusStore jobStatusStore,
                                     EmrServerlessPlatformExecutor executor,
                                     AmazonS3 s3Client) {
        this.properties = properties;
        this.jobStatusStore = jobStatusStore;
        this.executor = executor;
        this.s3Client = s3Client;
    }

    public void sendJob(BulkImportJob job) {
        String jobRunId = UUID.randomUUID().toString();
        jobStatusStore.jobValidated(ingestJobAccepted(job.toIngestJob(), Instant.now())
                .jobRunId(jobRunId).build());
        s3Client.putObject(properties.get(BULK_IMPORT_BUCKET),
                "bulk_import/" + job.getId() + "-" + jobRunId + ".json",
                new BulkImportJobSerDe().toJson(job));
        executor.runJobOnPlatform(BulkImportArguments.builder()
                .bulkImportJob(job)
                .jobRunId(jobRunId)
                .instanceProperties(properties)
                .build());
    }
}

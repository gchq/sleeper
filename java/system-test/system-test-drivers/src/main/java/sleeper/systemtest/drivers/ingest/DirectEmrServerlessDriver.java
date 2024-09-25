/*
 * Copyright 2022-2024 Crown Copyright
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
import sleeper.ingest.job.status.IngestJobStatusStore;
import sleeper.ingest.status.store.job.IngestJobStatusStoreFactory;
import sleeper.systemtest.drivers.util.SystemTestClients;
import sleeper.systemtest.dsl.ingest.DirectBulkImportDriver;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;

import java.time.Instant;
import java.util.UUID;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_BUCKET;
import static sleeper.ingest.job.status.IngestJobValidatedEvent.ingestJobAccepted;

public class DirectEmrServerlessDriver implements DirectBulkImportDriver {
    private final SystemTestInstanceContext instance;
    private final AmazonS3 s3Client;
    private final AmazonDynamoDB dynamoDBClient;
    private final EmrServerlessClient emrClient;

    public DirectEmrServerlessDriver(SystemTestInstanceContext instance, SystemTestClients clients) {
        this.instance = instance;
        this.s3Client = clients.getS3();
        this.dynamoDBClient = clients.getDynamoDB();
        this.emrClient = clients.getEmrServerless();
    }

    public void sendJob(BulkImportJob job) {
        String jobRunId = UUID.randomUUID().toString();
        jobStatusStore().jobValidated(ingestJobAccepted(job.toIngestJob(), Instant.now())
                .jobRunId(jobRunId).build());
        s3Client.putObject(instance.getInstanceProperties().get(BULK_IMPORT_BUCKET),
                "bulk_import/" + job.getId() + "-" + jobRunId + ".json",
                new BulkImportJobSerDe().toJson(job));
        executor().runJobOnPlatform(BulkImportArguments.builder()
                .bulkImportJob(job)
                .jobRunId(jobRunId)
                .instanceProperties(instance.getInstanceProperties())
                .build());
    }

    private IngestJobStatusStore jobStatusStore() {
        return IngestJobStatusStoreFactory.getStatusStore(dynamoDBClient, instance.getInstanceProperties());
    }

    private EmrServerlessPlatformExecutor executor() {
        return new EmrServerlessPlatformExecutor(emrClient, instance.getInstanceProperties());
    }
}

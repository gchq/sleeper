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

package sleeper.systemtest.drivers.ingest;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.emrserverless.EmrServerlessClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import sleeper.bulkimport.core.job.BulkImportJob;
import sleeper.bulkimport.core.job.BulkImportJobSerDe;
import sleeper.bulkimport.starter.executor.BulkImportArguments;
import sleeper.bulkimport.starter.executor.EmrServerlessPlatformExecutor;
import sleeper.core.tracker.ingest.job.IngestJobTracker;
import sleeper.ingest.trackerv2.job.IngestJobTrackerFactory;
import sleeper.systemtest.drivers.util.SystemTestClients;
import sleeper.systemtest.dsl.ingest.DirectBulkImportDriver;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;

import java.time.Instant;
import java.util.UUID;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_BUCKET;

public class DirectEmrServerlessDriver implements DirectBulkImportDriver {
    private final SystemTestInstanceContext instance;
    private final S3Client s3Client;
    private final DynamoDbClient dynamoClient;
    private final EmrServerlessClient emrClient;

    public DirectEmrServerlessDriver(SystemTestInstanceContext instance, SystemTestClients clients) {
        this.instance = instance;
        this.s3Client = clients.getS3();
        this.dynamoClient = clients.getDynamo();
        this.emrClient = clients.getEmrServerless();
    }

    public void sendJob(BulkImportJob job) {
        String jobRunId = UUID.randomUUID().toString();
        jobTracker().jobValidated(job.toIngestJob().acceptedEventBuilder(Instant.now())
                .jobRunId(jobRunId).build());
        s3Client.putObject(PutObjectRequest.builder()
                .bucket(instance.getInstanceProperties().get(BULK_IMPORT_BUCKET))
                .key("bulk_import/" + job.getId() + "-" + jobRunId + ".json")
                .build(),
                RequestBody.fromString(new BulkImportJobSerDe().toJson(job)));
        executor().runJobOnPlatform(BulkImportArguments.builder()
                .bulkImportJob(job)
                .jobRunId(jobRunId)
                .instanceProperties(instance.getInstanceProperties())
                .build());
    }

    private IngestJobTracker jobTracker() {
        return IngestJobTrackerFactory.getTracker(dynamoClient, instance.getInstanceProperties());
    }

    private EmrServerlessPlatformExecutor executor() {
        return new EmrServerlessPlatformExecutor(emrClient, instance.getInstanceProperties());
    }
}

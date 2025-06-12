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
package sleeper.bulkimport.starter.executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import sleeper.bulkimport.core.job.BulkImportJob;
import sleeper.bulkimport.core.job.BulkImportJobSerDe;
import sleeper.core.properties.instance.InstanceProperties;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_BUCKET;

public class BulkImportJobWriterToS3 implements BulkImportExecutor.WriteJobToBucket {
    public static final Logger LOGGER = LoggerFactory.getLogger(BulkImportJobWriterToS3.class);

    protected final InstanceProperties instanceProperties;
    protected final S3Client s3Client;

    public BulkImportJobWriterToS3(InstanceProperties instanceProperties, S3Client s3Client) {
        this.instanceProperties = instanceProperties;
        this.s3Client = s3Client;
    }

    @Override
    public void writeJobToBulkImportBucket(BulkImportJob bulkImportJob, String jobRunID) {
        String bulkImportBucket = instanceProperties.get(BULK_IMPORT_BUCKET);
        if (null == bulkImportBucket) {
            throw new RuntimeException("sleeper.bulk.import.bucket was not set. Has one of the bulk import stacks been deployed?");
        }
        String key = "bulk_import/" + bulkImportJob.getId() + "-" + jobRunID + ".json";
        String bulkImportJobJSON = new BulkImportJobSerDe().toJson(bulkImportJob);
        s3Client.putObject(PutObjectRequest.builder()
                .bucket(bulkImportBucket)
                .key(key)
                .build(),
                RequestBody.fromString(bulkImportJobJSON));
        LOGGER.info("Put object for job {} to key {} in bucket {}", bulkImportJob.getId(), key, bulkImportBucket);
    }
}

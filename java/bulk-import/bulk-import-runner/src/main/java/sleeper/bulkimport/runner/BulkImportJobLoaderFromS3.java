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
package sleeper.bulkimport.runner;

import com.google.gson.JsonSyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import sleeper.bulkimport.core.job.BulkImportJob;
import sleeper.bulkimport.core.job.BulkImportJobSerDe;
import sleeper.core.properties.instance.InstanceProperties;

import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_BUCKET;

public class BulkImportJobLoaderFromS3 {

    public static final Logger LOGGER = LoggerFactory.getLogger(BulkImportJobLoaderFromS3.class);

    private BulkImportJobLoaderFromS3() {

    }

    public static BulkImportJob loadJob(InstanceProperties instanceProperties, String jobId, String jobRunId, S3Client s3Client) {
        String bulkImportBucket = instanceProperties.get(BULK_IMPORT_BUCKET);
        if (null == bulkImportBucket) {
            throw new RuntimeException("sleeper.bulk.import.bucket was not set. Has one of the bulk import stacks been deployed?");
        }
        String jsonJobKey = "bulk_import/" + jobId + "-" + jobRunId + ".json";
        LOGGER.info("Loading bulk import job from key {} in bulk import bucket {}", jsonJobKey, bulkImportBucket);
        String jsonJob = s3Client.getObjectAsBytes(GetObjectRequest.builder()
                .bucket(bulkImportBucket)
                .key(jsonJobKey)
                .build()).asUtf8String();
        try {
            return new BulkImportJobSerDe().fromJson(jsonJob);
        } catch (JsonSyntaxException e) {
            LOGGER.error("Json job was malformed");
            throw e;
        } finally {
            s3Client.deleteObject(DeleteObjectRequest.builder()
                    .bucket(bulkImportBucket)
                    .key(jsonJobKey)
                    .build());
        }
    }
}

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

package sleeper.systemtest.drivers.sourcedata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.S3Object;

import sleeper.configuration.properties.S3InstanceProperties;
import sleeper.systemtest.dsl.instance.DeployedSystemTestResources;
import sleeper.systemtest.dsl.sourcedata.GeneratedIngestSourceFiles;
import sleeper.systemtest.dsl.sourcedata.GeneratedIngestSourceFilesDriver;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.function.Predicate.not;

public class AwsGeneratedIngestSourceFilesDriver implements GeneratedIngestSourceFilesDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(AwsGeneratedIngestSourceFilesDriver.class);

    private final DeployedSystemTestResources context;
    private final S3Client s3Client;

    public AwsGeneratedIngestSourceFilesDriver(DeployedSystemTestResources context, S3Client s3Client) {
        this.context = context;
        this.s3Client = s3Client;
    }

    public GeneratedIngestSourceFiles findGeneratedFiles() {
        String bucketName = context.getSystemTestBucketName();
        List<String> objectKeys = s3Client.listObjectsV2Paginator(builder -> builder.bucket(bucketName).prefix("ingest/"))
                .contents().stream()
                .map(S3Object::key)
                .collect(Collectors.toUnmodifiableList());
        LOGGER.info("Found ingest objects in source bucket: {}", objectKeys.size());
        return new GeneratedIngestSourceFiles(bucketName, objectKeys);
    }

    public void emptyBucket() {
        String bucketName = context.getSystemTestBucketName();
        List<ObjectIdentifier> objects = s3Client.listObjectsV2Paginator(builder -> builder.bucket(bucketName))
                .contents().stream().map(S3Object::key)
                .filter(not(S3InstanceProperties.S3_INSTANCE_PROPERTIES_FILE::equals))
                .map(key -> ObjectIdentifier.builder().key(key).build())
                .collect(Collectors.toList());
        if (!objects.isEmpty()) {
            s3Client.deleteObjects(builder -> builder.bucket(bucketName)
                    .delete(deleteBuilder -> deleteBuilder.objects(objects)));
        }
    }
}

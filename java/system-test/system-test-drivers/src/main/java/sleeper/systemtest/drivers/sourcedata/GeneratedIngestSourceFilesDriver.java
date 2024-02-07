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

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.systemtest.drivers.instance.SystemTestDeploymentContext;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.function.Predicate.not;

public class GeneratedIngestSourceFilesDriver {
    private static final Logger LOGGER = LoggerFactory.getLogger(GeneratedIngestSourceFilesDriver.class);

    private final SystemTestDeploymentContext context;
    private final S3Client s3Client;

    public GeneratedIngestSourceFilesDriver(SystemTestDeploymentContext context, S3Client s3Client) {
        this.context = context;
        this.s3Client = s3Client;
    }

    public GeneratedIngestSourceFiles findGeneratedFiles() {
        String bucketName = context.getSystemTestBucketName();
        List<S3Object> objects = s3Client.listObjectsV2Paginator(builder ->
                        builder.bucket(bucketName).prefix("ingest/"))
                .contents().stream().collect(Collectors.toUnmodifiableList());
        LOGGER.info("Found ingest objects in source bucket: {}", objects.size());
        return new GeneratedIngestSourceFiles(bucketName, objects);
    }

    public void emptyBucket() {
        String bucketName = context.getSystemTestBucketName();
        List<ObjectIdentifier> objects = s3Client.listObjectsV2Paginator(builder -> builder.bucket(bucketName))
                .contents().stream().map(S3Object::key)
                .filter(not(InstanceProperties.S3_INSTANCE_PROPERTIES_FILE::equals))
                .map(key -> ObjectIdentifier.builder().key(key).build())
                .collect(Collectors.toList());
        if (!objects.isEmpty()) {
            s3Client.deleteObjects(builder -> builder.bucket(bucketName)
                    .delete(deleteBuilder -> deleteBuilder.objects(objects)));
        }
    }
}

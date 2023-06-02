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

package sleeper.clients.teardown;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectVersionsResponse;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;

import java.util.Collection;
import java.util.stream.Collectors;

public class RemoveJarsBucket {
    private static final Logger LOGGER = LoggerFactory.getLogger(RemoveJarsBucket.class);

    private RemoveJarsBucket() {
    }

    public static void removeWithInstanceId(S3Client s3, String instanceId) {
        remove(s3, "sleeper-" + instanceId + "-jars");
    }

    public static void remove(S3Client s3, String bucketName) {
        LOGGER.info("Emptying bucket {}", bucketName);
        s3.listObjectVersionsPaginator(builder -> builder.bucket(bucketName))
                .stream().parallel()
                .forEach(response -> deleteVersions(s3, bucketName, response));
        LOGGER.info("Deleting bucket {}", bucketName);
        s3.deleteBucket(builder -> builder.bucket(bucketName));
    }

    private static void deleteVersions(S3Client s3, String bucketName, ListObjectVersionsResponse response) {
        if (!response.versions().isEmpty()) {
            LOGGER.info("Deleting {} versions", response.versions().size());
            s3.deleteObjects(builder -> builder.bucket(bucketName)
                    .delete(deleteBuilder -> deleteBuilder
                            .objects(objectIdentifiers(response))));
        }
    }

    private static Collection<ObjectIdentifier> objectIdentifiers(ListObjectVersionsResponse response) {
        return response.versions().stream()
                .map(version -> ObjectIdentifier.builder().key(version.key()).versionId(version.versionId()).build())
                .collect(Collectors.toList());
    }
}

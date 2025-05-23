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

package sleeper.clients.deploy.localstack;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;

public class TearDownBucket {
    private TearDownBucket() {
    }

    public static void emptyAndDelete(S3Client s3Client, String bucketName) {
        s3Client.listObjectsV2Paginator(request -> request.bucket(bucketName))
                .forEach((ListObjectsV2Response response) -> {
                    if (response.hasContents() && !response.contents().isEmpty()) {
                        s3Client.deleteObjects(request -> request
                                .bucket(bucketName)
                                .delete(delete -> delete.objects(response.contents().stream()
                                        .map(object -> ObjectIdentifier.builder()
                                                .key(object.key())
                                                .build())
                                        .toList())));
                    }
                });
        s3Client.deleteBucket(request -> request.bucket(bucketName));
    }
}

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
package sleeper.systemtest.configuration;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import static sleeper.systemtest.configuration.SystemTestProperty.SYSTEM_TEST_BUCKET_NAME;

public class SystemTestDataGenerationJobStore {
    private final SystemTestPropertyValues properties;
    private final S3Client s3Client;
    private final SystemTestDataGenerationJobSerDe serDe = new SystemTestDataGenerationJobSerDe();

    public SystemTestDataGenerationJobStore(SystemTestPropertyValues properties, S3Client s3Client) {
        this.properties = properties;
        this.s3Client = s3Client;
    }

    public String writeJobGetObjectKey(SystemTestDataGenerationJob job) {
        String key = "jobs/" + job.getJobId() + ".json";
        String json = serDe.toJson(job);
        s3Client.putObject(
                PutObjectRequest.builder()
                        .bucket(properties.get(SYSTEM_TEST_BUCKET_NAME))
                        .key(key)
                        .build(),
                RequestBody.fromString(json));
        return key;
    }

    public SystemTestDataGenerationJob readJob(String objectKey) {
        ResponseBytes<GetObjectResponse> response = s3Client.getObjectAsBytes(
                GetObjectRequest.builder()
                        .bucket(properties.get(SYSTEM_TEST_BUCKET_NAME))
                        .key(objectKey)
                        .build());
        return serDe.fromJson(response.asUtf8String());
    }

}

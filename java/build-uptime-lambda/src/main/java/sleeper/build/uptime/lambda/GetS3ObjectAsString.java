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
package sleeper.build.uptime.lambda;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.util.Optional;

@FunctionalInterface
public interface GetS3ObjectAsString {

    Optional<String> getS3ObjectAsString(String bucket, String key);

    static GetS3ObjectAsString fromClient(S3Client s3) {
        return (bucket, key) -> {
            try {
                String json = s3.getObjectAsBytes(builder -> builder.bucket(bucket).key(key)).asUtf8String();
                return Optional.of(json);
            } catch (NoSuchKeyException e) {
                return Optional.empty();
            }
        };
    }

}

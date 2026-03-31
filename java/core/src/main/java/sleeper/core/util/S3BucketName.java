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
package sleeper.core.util;

import software.amazon.awssdk.services.sts.StsClient;

import java.util.Locale;

/**
 * Build a bucket name to use on S3.
 */
public class S3BucketName {
    private S3BucketName() {
    }

    /**
     * Build an S3 Bucket name.
     *
     * @param  instanceId the AWS instance id
     * @param  args       elements to include in the bucket name
     * @return            an S3 bucket name
     */
    public static String parse(String instanceId, String... args) {
        try (StsClient stsClient = StsClient.create()) {
            return parse(stsClient, instanceId, args);
        }
    }

    /**
     * Build an S3 Bucket name.
     *
     * @param  stsClient  the AWS StsClient
     * @param  instanceId the AWS instance id
     * @param  args       elements to include in the bucket name
     * @return            an S3 bucket name
     */
    public static String parse(StsClient stsClient, String instanceId, String... args) {
        String account = stsClient.getCallerIdentity(r -> r.build()).account();

        String namePortion = String.join("-", args);

        String bucketName = String.join("-", "sleeper", instanceId,
                namePortion, account).toLowerCase(Locale.ROOT);

        if (bucketName.length() > 63) {
            throw new IllegalArgumentException("Complete bucket name exceeds 63 characters.");
        }

        if (namePortion.length() > 20) {
            throw new IllegalArgumentException("Name portion exceeds 20 characters.");
        }

        return bucketName;
    }

}

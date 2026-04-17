/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.cdk.util;

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
     * @param  account     the AWS account
     * @param  instanceId  the AWS instance id
     * @param  namePortion the name identifying this bucket within the Sleeper instance (must not exceed 20 characters)
     * @return             an S3 bucket name
     */
    public static String parse(String account, String instanceId, String namePortion) {

        if (namePortion.length() > 20) {
            throw new IllegalArgumentException("Name portion exceeds 20 characters.");
        }

        String bucketName = String.join("-", "sleeper", instanceId,
                namePortion, account).toLowerCase(Locale.ROOT);

        if (bucketName.length() > 63) {
            throw new IllegalArgumentException("Complete bucket name exceeds 63 characters.");
        }

        return bucketName;
    }

}

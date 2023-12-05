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

import software.amazon.awssdk.services.s3.S3Client;

import sleeper.core.util.PollWithRetries;

import java.time.Duration;

public class WaitForS3BucketToEmpty {
    private static final PollWithRetries DEFAULT_POLL = PollWithRetries
            .intervalAndPollingTimeout(Duration.ofSeconds(5), Duration.ofMinutes(1));

    private final PollWithRetries poll;
    private final String bucketName;
    private final S3Client s3;

    public WaitForS3BucketToEmpty(S3Client s3, String bucketName, PollWithRetries poll) {
        this.s3 = s3;
        this.bucketName = bucketName;
        this.poll = poll;
    }

    public static WaitForS3BucketToEmpty from(S3Client s3, String bucketName) {
        return new WaitForS3BucketToEmpty(s3, bucketName, DEFAULT_POLL);
    }

    public void pollUntilFinished() throws InterruptedException {
        poll.pollUntil("bucket to be empty", this::hasBucketEmptied);
    }

    private boolean hasBucketEmptied() {
        return s3.listObjectVersions(builder -> builder.bucket(bucketName)).versions().isEmpty();
    }
}

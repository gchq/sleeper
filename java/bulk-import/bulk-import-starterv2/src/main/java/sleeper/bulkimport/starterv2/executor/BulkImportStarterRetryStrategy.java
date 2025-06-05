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
package sleeper.bulkimport.starterv2.executor;

import software.amazon.awssdk.core.client.builder.SdkClientBuilder;
import software.amazon.awssdk.retries.LegacyRetryStrategy;
import software.amazon.awssdk.retries.api.BackoffStrategy;
import software.amazon.awssdk.retries.api.RetryStrategy;

import java.time.Duration;

/**
 * A strategy for retrying attempts to start a bulk import job. Customised to handle rate limits for job creation, and
 * stay within the timeout of the bulk import starter lambda. Based on a lambda timeout of 2 minutes.
 */
public class BulkImportStarterRetryStrategy {

    private BulkImportStarterRetryStrategy() {
    }

    /**
     * Overrides the retry strategy and builds an AWS client.
     *
     * @param  <B>     the type of the AWS client builder
     * @param  <C>     the type of the AWS client
     * @param  builder the AWS client builder
     * @return         the AWS client
     */
    public static <B extends SdkClientBuilder<B, C>, C> C overrideAndBuildAwsClient(B builder) {
        builder.overrideConfiguration(config -> config.retryStrategy(create()));
        return builder.build();
    }

    private static RetryStrategy create() {
        // Start with default AWS client retry strategy
        return LegacyRetryStrategy.builder()
                // Setting this close to the sustained rate of EMR API of 0.5 requests/second
                // https://docs.aws.amazon.com/general/latest/gr/emr.html
                // Also close to EMR Serverless API limit of 1 request/second
                // https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/endpoints-quotas.html
                .throttlingBackoffStrategy(BackoffStrategy.exponentialDelayHalfJitter(
                        Duration.ofSeconds(2), Duration.ofSeconds(20)))
                // Delay before jitter:
                // 2, 4, 8, 16, 20, 20, 20...
                // 30s in first 4 retries, then 20s per retry
                // Averages, with half jitter:
                // 1.5, 3, 6, 12, 15, 15, 15...
                // 22.5s in first 4 retries, then 15s per retry
                // Total wait time:
                // r is the number of retries
                // w is how long we wait for on average if we're throttled indefinitely
                // m is our maximum wait time if we're throttled indefinitely, if jitter always gives the maximum value
                // w = 22.5 + (r - 4) * 15
                // m = 30 + (r - 4) * 20
                // We can set m to stay within a lambda timeout, and calculate r.
                // m = 30 + 20r - 80
                // m = 20r - 50
                // m + 50 = 20r
                // r = (m + 50) / 20
                // Setting m to 2 minutes for the lambda timeout:
                // m = 2 * 60 = 120
                // r = (120 + 50) / 20 = 8.5
                // Round that down to 8, then add 1 to convert from retries to attempts.
                // Let's calculate the effective maximum and average wait times when throttled indefinitely:
                // m = 30 + (8 - 4) * 20 = 110s = 1m 50s
                // w = 22.5 + (8 - 4) * 15 = 82.5s = 1m 22.5s
                .maxAttempts(9)
                .build();
    }

}

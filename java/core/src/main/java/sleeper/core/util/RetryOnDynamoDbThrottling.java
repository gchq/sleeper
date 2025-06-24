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

/**
 * Performs some operation with retries if requests to the DynamoDB API are throttled. This is implemented in a separate
 * module, in the class DynamoDBUtils.
 */
@FunctionalInterface
public interface RetryOnDynamoDbThrottling {

    /**
     * Retries a given operation with wait time between retries, if it results in a throttling exception from the
     * DynamoDB API.
     *
     * @param  pollWithRetries      the settings for retries and waits
     * @param  runnable             the operation to retry
     * @throws InterruptedException if the thread is interrupted during the retries
     */
    void retryOnThrottlingException(PollWithRetries pollWithRetries, Runnable runnable) throws InterruptedException;

}

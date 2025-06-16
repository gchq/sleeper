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
 * Helpers to create fake implementations of RetryOnDynamoDbThrottling for tests.
 */
public class RetryOnDynamoDbThrottlingTestHelper {

    private RetryOnDynamoDbThrottlingTestHelper() {
    }

    /**
     * Creates an instance of RetryOnDynamoDbThrottling that will never retry.
     *
     * @return the object
     */
    public static RetryOnDynamoDbThrottling noRetriesOnThrottling() {
        return (poll, runnable) -> runnable.run();
    }

    /**
     * Creates an instance of RetryOnDynamoDbThrottling that will retry if a particular exception is thrown. This
     * includes if an exception is thrown which was the cause of the given exception.
     *
     * @param  throttlingException the exception to treat as a throttling exception
     * @return                     the object
     */
    public static RetryOnDynamoDbThrottling retryOnThrottlingException(RuntimeException throttlingException) {
        return (poll, runnable) -> {
            poll.pollUntil("no throttling exception", () -> {
                try {
                    runnable.run();
                    return true;
                } catch (RuntimeException e) {
                    if (isThrottlingException(e, throttlingException)) {
                        return false;
                    }
                    throw e;
                }
            });
        };
    }

    private static boolean isThrottlingException(RuntimeException thrown, RuntimeException throttlingException) {
        Throwable e = thrown;
        do {
            if (e == throttlingException) {
                return true;
            }
            e = e.getCause();
        } while (e != null);
        return false;
    }

}

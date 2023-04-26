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
package sleeper.clients.util;

import java.util.function.BooleanSupplier;

public class PollWithRetries {

    private final long pollIntervalMillis;
    private final int maxPolls;

    private PollWithRetries(long pollIntervalMillis, int maxPolls) {
        this.pollIntervalMillis = pollIntervalMillis;
        this.maxPolls = maxPolls;
    }

    public static PollWithRetries intervalAndMaxPolls(long pollIntervalMillis, int maxPolls) {
        return new PollWithRetries(pollIntervalMillis, maxPolls);
    }

    public void pollUntil(String description, BooleanSupplier checkFinished) throws InterruptedException {
        int polls = 0;
        while (!checkFinished.getAsBoolean()) {
            polls++;
            if (polls >= maxPolls) {
                throw new TimedOutException("Timed out waiting until " + description);
            }
            Thread.sleep(pollIntervalMillis);
        }
    }

    public static class TimedOutException extends RuntimeException {
        private TimedOutException(String message) {
            super(message);
        }
    }
}

/*
 * Copyright 2022-2024 Crown Copyright
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

import java.time.Duration;
import java.util.List;

/**
 * Helpers to create test fakes for ThreadSleep.
 */
public class ThreadSleepTestHelper {

    private ThreadSleepTestHelper() {
    }

    /**
     * Creates an implementation of a waiter that records the wait times in a list.
     *
     * @param  recordWaits the list to store wait times
     * @return             a {@link ThreadSleep} that records wait times
     */
    public static ThreadSleep recordWaits(List<Duration> recordWaits) {
        return millis -> recordWaits.add(Duration.ofMillis(millis));
    }

    /**
     * Creates an implementation of a waiter that performs multiple actions.
     *
     * @param  waiters actions to perform
     * @return         a {@link ThreadSleep} that performs the given actions
     */
    public static ThreadSleep multipleWaitActions(ThreadSleep... waiters) {
        return millis -> {
            for (ThreadSleep waiter : waiters) {
                waiter.waitForMillis(millis);
            }
        };
    }

    /**
     * Creates an implementation of a waiter that does nothing.
     *
     * @return a {@link ThreadSleep} that does nothing
     */
    public static ThreadSleep noWaits() {
        return millis -> {
        };
    }

    /**
     * Extends an implementation of a waiter to also perform another action.
     *
     * @param  waiter the waiter to extend
     * @param  action the action to perform
     * @return        a waiter which will behave like the original waiter but perform the action first
     */
    public static ThreadSleep withActionAfterWait(ThreadSleep waiter, WaitAction action) {
        return millis -> {
            try {
                action.run();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            waiter.waitForMillis(millis);
        };
    }

    /**
     * An action to perform during a wait.
     */
    public interface WaitAction {

        /**
         * Perform the action.
         *
         * @throws Exception if anything went wrong
         */
        void run() throws Exception;
    }
}

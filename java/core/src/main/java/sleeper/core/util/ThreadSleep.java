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

/**
 * Waits for a number of milliseconds. Implemented by <code>Thread.sleep</code>.
 */
@FunctionalInterface
public interface ThreadSleep {
    /**
     * Wait for the specified period.
     *
     * @param  milliseconds         milliseconds to wait for
     * @throws InterruptedException if the thread is interrupted while waiting
     */
    void waitForMillis(long milliseconds) throws InterruptedException;
}

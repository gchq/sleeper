/*
 * Copyright 2022 Crown Copyright
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
package sleeper.job.common.action.thread;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import sleeper.job.common.action.Action;

public class PeriodicActionRunnableTest {

    @Test
    public void shouldRunInBackground() throws InterruptedException {
        // Given
        KeepAlive keepAlive = new KeepAlive();
        PeriodicActionRunnable runnable = new PeriodicActionRunnable(keepAlive, 1);
        runnable.start();

        // When
        Thread.sleep(5000L);
        runnable.stop();

        // Then
        assertTrue(keepAlive.getCount() >= 4);
    }

    @Test
    public void shouldStopActionImmediately() throws InterruptedException {
        // Given
        KeepAlive keepAlive = new KeepAlive();
        PeriodicActionRunnable runnable = new PeriodicActionRunnable(keepAlive, 5);
        runnable.start();
        long started = System.currentTimeMillis();

        // When
        Thread.sleep(250);
        runnable.stop();

        // Then
        assertEquals(0, keepAlive.getCount());
        assertTrue(System.currentTimeMillis() - started < 2 * 1000);
    }

    private static class KeepAlive implements Action {
        private int i;

        public KeepAlive() {
            this.i = 0;
        }

        @Override
        public void call() {
            i++;
        }

        public int getCount() {
            return i;
        }
    }
}

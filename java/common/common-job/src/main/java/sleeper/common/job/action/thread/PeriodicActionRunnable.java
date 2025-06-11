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
package sleeper.common.job.action.thread;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.common.job.action.Action;
import sleeper.common.job.action.ActionException;

/**
 * Calls an action every N seconds, sleeping in between calls. Used for keep-alive calls to SQS.
 */
public class PeriodicActionRunnable implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(PeriodicActionRunnable.class);

    private final Action action;
    private final long keepAlivePeriodInMilliseconds;
    private volatile Thread thread;

    public PeriodicActionRunnable(Action action, int keepAlivePeriodInSeconds) {
        this.action = action;
        this.keepAlivePeriodInMilliseconds = keepAlivePeriodInSeconds * 1000L;
        this.thread = new Thread(this);
    }

    public void start() {
        thread.start();
    }

    @Override
    public void run() {
        Thread thisThread = Thread.currentThread();

        while (thisThread == thread) {
            try {
                LOGGER.info("PeriodicActionRunnable is sleeping for {} milliseconds", keepAlivePeriodInMilliseconds);
                Thread.sleep(keepAlivePeriodInMilliseconds);
            } catch (InterruptedException ex) {
                return;
            }
            try {
                action.call();
            } catch (ActionException e) {
                throw new RuntimeException("ActionException calling PeriodicActionRunnable " + action, e);
            }
        }
    }

    public void stop() {
        if (thread != null) {
            Thread theThread = thread;
            thread = null;
            if (theThread.isAlive()) {
                theThread.interrupt();
            }
            try {
                theThread.join();
            } catch (InterruptedException e) {
            }
        }
    }

}

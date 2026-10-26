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
package sleeper.compaction.job.execution;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.core.task.CompactionTaskTerminator;

import java.time.Duration;

/**
 * Terminates a compaction task by exiting the JVM. This is used when a compaction stalls in native code, as the native
 * code shares memory with the JVM and we cannot assume the process is in a consistent state.
 * <p>
 * Shutdown hooks are given a limited time to run, after which the JVM is halted, in case a shutdown hook also hangs.
 */
public class ExitJvmCompactionTaskTerminator implements CompactionTaskTerminator {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExitJvmCompactionTaskTerminator.class);
    private static final int EXIT_CODE = 1;

    private final Duration haltAfter;

    public ExitJvmCompactionTaskTerminator() {
        this(Duration.ofSeconds(30));
    }

    public ExitJvmCompactionTaskTerminator(Duration haltAfter) {
        this.haltAfter = haltAfter;
    }

    @Override
    @SuppressFBWarnings("DM_EXIT")
    public void terminate(String reason) {
        LOGGER.error("Terminating compaction task: {}", reason);
        Thread halt = new Thread(() -> {
            try {
                Thread.sleep(haltAfter.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            LOGGER.error("JVM did not exit within {}, halting", haltAfter);
            Runtime.getRuntime().halt(EXIT_CODE);
        }, "compaction-task-halt");
        halt.setDaemon(true);
        halt.start();
        // Exit on a separate thread, as System.exit blocks until shutdown hooks have finished
        Thread exit = new Thread(() -> System.exit(EXIT_CODE), "compaction-task-exit");
        exit.setDaemon(true);
        exit.start();
    }
}

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
package sleeper.bulkexport.taskexecution;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.util.LoggedDuration;

import java.time.Instant;

/**
 * Main class to run the ECS bulk export task.
 */
public class ECSBulkExportTaskRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(ECSBulkExportTaskRunner.class);

    private ECSBulkExportTaskRunner() {
    }

    /**
     * Main method to run the task.
     *
     * @param args command line arguments
     */
    public static void main(String[] args) {
        Instant startTime = Instant.now();

        // This will be replaced with the actual task execution logic
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.error("Thread was interrupted", e);
        }
        LOGGER.info("Total run time = {}", LoggedDuration.withFullOutput(startTime, Instant.now()));
    }
}

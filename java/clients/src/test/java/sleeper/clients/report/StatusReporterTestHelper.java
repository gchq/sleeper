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

package sleeper.clients.report;

import static sleeper.core.testutils.SupplierTestHelper.exampleUUID;

/**
 * Helpers for testing reports on status held in trackers.
 */
public class StatusReporterTestHelper {
    private StatusReporterTestHelper() {
    }

    /**
     * Creates an example job ID based on a number. Note that jobs are not usually numbered in reality.
     *
     * @param  number the number of the job
     * @return        the job ID
     */
    public static String job(int number) {
        return exampleUUID("job", number);
    }

    /**
     * Creates an example task ID based on a number. Note that tasks are not usually numbered in reality.
     *
     * @param  number the number of the task
     * @return        the task ID
     */
    public static String task(int number) {
        return exampleUUID("task", number);
    }
}

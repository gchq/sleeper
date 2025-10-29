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
package sleeper.systemtest.dsl.bulkexport;

import sleeper.systemtest.dsl.SystemTestContext;
import sleeper.systemtest.dsl.SystemTestDrivers;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;

public class SystemTestBulkExport {
    SystemTestInstanceContext instance;
    BulkExportDriver driver;

    public SystemTestBulkExport(SystemTestContext context, SystemTestDrivers baseDrivers) {
        this.instance = context.instance();
        driver = baseDrivers.bulkExport(context);
        /*
         * this.instance = context.instance();
         * this.sourceFiles = context.sourceFiles();
         * SystemTestDrivers drivers = instance.adminDrivers();
         * driver = drivers.compaction(context);
         * pollDriver = drivers.pollWithRetries();
         * waitForJobCreation = new WaitForCompactionJobCreation(instance, driver);
         * waitForJobs = drivers.waitForCompaction(context);
         * // Use base driver to drain compaction queue as admin role does not have permission to do this
         * baseDriver = baseDrivers.compaction(context);
         */
    }

}

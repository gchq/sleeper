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

package sleeper.systemtest.dsl.gc;

import sleeper.core.properties.table.TableProperties;
import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.dsl.SystemTestContext;
import sleeper.systemtest.dsl.SystemTestDrivers;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;
import sleeper.systemtest.dsl.util.PollWithRetriesDriver;

import java.time.Duration;
import java.util.List;
import java.util.stream.Stream;

public class GarbageCollectionDsl {

    private static final PollWithRetries DEFAULT_WAIT_FOR_GC = PollWithRetries.intervalAndPollingTimeout(
            Duration.ofSeconds(5), Duration.ofMinutes(2));

    private final GarbageCollectionDriver driver;
    private final PollWithRetriesDriver pollDriver;
    private final SystemTestInstanceContext instance;

    public GarbageCollectionDsl(SystemTestContext context) {
        SystemTestDrivers instanceAdminDrivers = context.instance().adminRoleDrivers();
        driver = instanceAdminDrivers.garbageCollection(context);
        pollDriver = instanceAdminDrivers.pollWithRetries();
        instance = context.instance();
    }

    public GarbageCollectionDsl invoke() {
        driver.invokeGarbageCollection(instance.streamTableProperties().map(TableProperties::getStatus));
        return this;
    }

    public GarbageCollectionDsl invokeForTablesByDescriptor(Stream<String> tableDescriptors) {
        driver.invokeGarbageCollection(instance.streamTableStatusByDescriptors(tableDescriptors));
        return this;
    }

    public void waitFor() {
        waitFor(DEFAULT_WAIT_FOR_GC);
    }

    public void waitFor(PollWithRetries poll) {
        WaitForGC.waitUntilNoUnreferencedFiles(instance, pollDriver.poll(poll));
    }

    public void waitForTables(List<String> tableDescriptors) {
        waitForTables(tableDescriptors, DEFAULT_WAIT_FOR_GC);
    }

    public void waitForTables(List<String> tableDescriptors, PollWithRetries poll) {
        WaitForGC.waitUntilNoUnreferencedFiles(instance, tableDescriptors, pollDriver.poll(poll));
    }
}

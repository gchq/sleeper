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

package sleeper.systemtest.dsl.gc;

import sleeper.core.util.PollWithRetries;
import sleeper.systemtest.dsl.SystemTestContext;
import sleeper.systemtest.dsl.SystemTestDrivers;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;
import sleeper.systemtest.dsl.util.PollWithRetriesDriver;

import java.time.Duration;

public class SystemTestGarbageCollection {

    private final GarbageCollectionDriver driver;
    private final PollWithRetriesDriver pollDriver;
    private final SystemTestInstanceContext instance;

    public SystemTestGarbageCollection(SystemTestContext context) {
        SystemTestDrivers instanceAdminDrivers = context.instance().adminDrivers();
        driver = instanceAdminDrivers.garbageCollection(context);
        pollDriver = instanceAdminDrivers.pollWithRetries();
        instance = context.instance();
    }

    public SystemTestGarbageCollection invoke() {
        driver.invokeGarbageCollection();
        return this;
    }

    public void waitFor() {
        waitFor(PollWithRetries.intervalAndPollingTimeout(Duration.ofSeconds(5), Duration.ofMinutes(1)));
    }

    public void waitFor(PollWithRetries poll) {
        WaitForGC.waitUntilNoUnreferencedFiles(instance, pollDriver.poll(poll));
    }
}

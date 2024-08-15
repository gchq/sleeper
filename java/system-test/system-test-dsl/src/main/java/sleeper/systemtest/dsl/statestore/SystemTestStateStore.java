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
package sleeper.systemtest.dsl.statestore;

import sleeper.systemtest.dsl.SystemTestContext;
import sleeper.systemtest.dsl.SystemTestDrivers;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;

import java.time.Instant;
import java.util.Map;

import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;

public class SystemTestStateStore {

    private final SystemTestContext context;
    private final StateStoreCommitterDriver driver;
    private final StateStoreCommitterLogsDriver logsDriver;

    public SystemTestStateStore(SystemTestContext context) {
        this.context = context;
        SystemTestDrivers adminDrivers = context.instance().adminDrivers();
        driver = adminDrivers.stateStoreCommitter(context);
        logsDriver = adminDrivers.stateStoreCommitterLogs(context);
    }

    public SystemTestStateStoreFakeCommits fakeCommits() {
        return new SystemTestStateStoreFakeCommits(context, driver, logsDriver);
    }

    public double commitsPerSecondForTable() {
        return commitsPerSecondByTableId().getOrDefault(
                context.instance().getTableProperties().get(TABLE_ID),
                0.0);
    }

    public Map<String, Double> commitsPerSecondByTable() {
        Map<String, Double> byTableId = commitsPerSecondByTableId();
        SystemTestInstanceContext instance = context.instance();
        return instance.streamTableProperties()
                .collect(toMap(
                        table -> instance.getTestTableName(table),
                        table -> byTableId.getOrDefault(table.get(TABLE_ID), 0.0)));
    }

    private Map<String, Double> commitsPerSecondByTableId() {
        return logsDriver.getLogsInPeriod(context.reporting().getRecordingStartTime(), Instant.now())
                .computeOverallCommitsPerSecondByTableId(
                        context.instance().streamTableProperties()
                                .map(table -> table.get(TABLE_ID))
                                .collect(toSet()));
    }

}

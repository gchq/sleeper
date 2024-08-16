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
package sleeper.systemtest.drivers.statestore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;

import sleeper.clients.status.report.statestore.QueryStateStoreCommitterLogs;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;
import sleeper.systemtest.dsl.statestore.StateStoreCommitterLogs;
import sleeper.systemtest.dsl.statestore.StateStoreCommitterLogsDriver;

import java.time.Instant;

public class AwsStateStoreCommitterLogsDriver implements StateStoreCommitterLogsDriver {
    public static final Logger LOGGER = LoggerFactory.getLogger(AwsStateStoreCommitterDriver.class);

    private final SystemTestInstanceContext instance;
    private final CloudWatchLogsClient cloudWatch;

    public AwsStateStoreCommitterLogsDriver(SystemTestInstanceContext instance, CloudWatchLogsClient cloudWatch) {
        this.instance = instance;
        this.cloudWatch = cloudWatch;
    }

    @Override
    public StateStoreCommitterLogs getLogsInPeriod(Instant startTime, Instant endTime) {
        QueryStateStoreCommitterLogs queryLogs = new QueryStateStoreCommitterLogs(instance.getInstanceProperties(), cloudWatch);
        return new StateStoreCommitterLogEntries(queryLogs.getLogsInPeriod(startTime, endTime));
    }
}

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

package sleeper.systemtest.drivers.partitioning;

import sleeper.clients.status.report.partitions.PartitionsStatusReporter;
import sleeper.splitter.core.status.PartitionsStatus;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;
import sleeper.systemtest.dsl.reporting.PartitionReportDriver;
import sleeper.systemtest.dsl.reporting.SystemTestReport;

public class AwsPartitionReportDriver implements PartitionReportDriver {

    private final SystemTestInstanceContext instance;

    public AwsPartitionReportDriver(SystemTestInstanceContext instance) {
        this.instance = instance;
    }

    public SystemTestReport statusReport() {
        return (out, startTime) -> new PartitionsStatusReporter(out)
                .report(PartitionsStatus.from(instance.getTableProperties(), instance.getStateStore()));
    }
}

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
package sleeper.systemtest.dsl.testutil.drivers;

import sleeper.core.metrics.TableMetrics;
import sleeper.systemtest.dsl.SystemTestContext;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;
import sleeper.systemtest.dsl.metrics.TableMetricsDriver;

import java.util.HashMap;
import java.util.Map;

import static sleeper.core.properties.instance.CommonProperty.ID;

public class InMemoryTableMetrics {

    private Map<String, TableMetrics> metricsByTableName = new HashMap<>();

    public TableMetricsDriver driver(SystemTestContext context) {
        return new Driver(context.instance());
    }

    private class Driver implements TableMetricsDriver {
        private final SystemTestInstanceContext instance;

        Driver(SystemTestInstanceContext instance) {
            this.instance = instance;
        }

        @Override
        public void generateTableMetrics() {
            String instanceId = instance.getInstanceProperties().get(ID);
            instance.streamTableProperties().forEach(table -> add(
                    TableMetrics.from(instanceId, table.getStatus(), instance.getStateStore(table))));
        }

        @Override
        public TableMetrics getTableMetrics() {
            return metricsByTableName.get(instance.getTableName());
        }
    }

    private void add(TableMetrics metrics) {
        metricsByTableName.put(metrics.getTableName(), metrics);
    }
}

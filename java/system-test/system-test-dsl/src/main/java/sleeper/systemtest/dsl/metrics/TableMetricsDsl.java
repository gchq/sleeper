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

package sleeper.systemtest.dsl.metrics;

import sleeper.core.metrics.TableMetrics;
import sleeper.core.properties.table.TableProperties;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;

public class TableMetricsDsl {

    private final SystemTestInstanceContext instance;
    private final TableMetricsDriver driver;

    public TableMetricsDsl(SystemTestInstanceContext instance, TableMetricsDriver driver) {
        this.instance = instance;
        this.driver = driver;
    }

    public TableMetricsDsl generate() {
        driver.triggerTableMetricsGeneration(instance.streamTableProperties().map(TableProperties::getStatus));
        return this;
    }

    public TableMetrics get() {
        return driver.getTableMetrics(instance.getTableProperties());
    }
}

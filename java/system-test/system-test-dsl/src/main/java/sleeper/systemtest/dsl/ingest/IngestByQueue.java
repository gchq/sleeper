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

package sleeper.systemtest.dsl.ingest;

import sleeper.core.properties.instance.InstanceProperty;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class IngestByQueue {
    private final SystemTestInstanceContext instance;
    private final IngestByQueueDriver driver;

    public IngestByQueue(SystemTestInstanceContext instance, IngestByQueueDriver driver) {
        this.instance = instance;
        this.driver = driver;
    }

    public String sendJobGetId(InstanceProperty queueUrlProperty, List<String> files) {
        return sendJobGetId(queueUrlProperty, instance.getTableName(), files);
    }

    public List<String> sendJobToAllTablesGetIds(InstanceProperty queueUrlProperty, List<String> files) {
        return instance.streamDeployedTableNames().parallel()
                .map(tableName -> sendJobGetId(queueUrlProperty, tableName, files))
                .collect(Collectors.toList());
    }

    public String sendJobGetId(InstanceProperty queueUrlProperty, String tableName, List<String> files) {
        String queueUrl = Objects.requireNonNull(instance.getInstanceProperties().get(queueUrlProperty),
                "queue URL property must be non-null: " + queueUrlProperty.getPropertyName());
        return driver.sendJobGetId(queueUrl, tableName, files);
    }
}

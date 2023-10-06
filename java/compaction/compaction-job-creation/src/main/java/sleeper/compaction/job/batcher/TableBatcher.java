/*
 * Copyright 2022-2023 Crown Copyright
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

package sleeper.compaction.job.batcher;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.configuration.properties.table.TableProperty;

import java.util.List;
import java.util.stream.Collectors;

import static sleeper.compaction.job.batcher.TableBatch.batchWithTables;
import static sleeper.configuration.properties.instance.CompactionProperty.TABLE_BATCHER_BATCH_SIZE;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.TABLE_BATCHER_QUEUE_URL;

public class TableBatcher {
    private final int batchSize;
    private final String queueUrl;
    private final List<TableProperties> tablePropertiesList;
    private final TableBatcherQueueClient queueClient;

    public TableBatcher(
            InstanceProperties instanceProperties, List<TableProperties> tablePropertiesList,
            TableBatcherQueueClient queueClient) {
        this.batchSize = instanceProperties.getInt(TABLE_BATCHER_BATCH_SIZE);
        this.queueUrl = instanceProperties.get(TABLE_BATCHER_QUEUE_URL);
        this.tablePropertiesList = tablePropertiesList;
        this.queueClient = queueClient;
    }

    public void batchTables() {
        for (int i = 0; i < tablePropertiesList.size(); i += batchSize) {
            List<String> tables = tablePropertiesList.subList(i, Math.min(i + batchSize, tablePropertiesList.size()))
                    .stream().map(tableProperties -> tableProperties.get(TableProperty.TABLE_NAME))
                    .collect(Collectors.toList());
            queueClient.send(queueUrl, batchWithTables(tables));
        }
    }
}

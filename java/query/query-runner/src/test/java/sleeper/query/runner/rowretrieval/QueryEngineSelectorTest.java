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
package sleeper.query.runner.rowretrieval;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.model.DataEngine;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TableProperty;
import sleeper.query.core.rowretrieval.LeafPartitionRowRetriever;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

public class QueryEngineSelectorTest {

    @Test
    public void shouldSelectJavaQueryPath() {
        // Given
        Configuration conf = new Configuration();
        ExecutorService exec = Executors.newSingleThreadExecutor();
        QueryEngineSelector selector = new QueryEngineSelector(exec, conf);
        InstanceProperties instanceProperties = new InstanceProperties();

        TableProperties tableProperties = new TableProperties(instanceProperties);
        tableProperties.setEnum(TableProperty.DATA_ENGINE, DataEngine.JAVA);

        // When
        LeafPartitionRowRetriever retriever = selector.getRowRetriever(tableProperties);

        // Then
        assertThat(retriever).isInstanceOf(LeafPartitionRowRetrieverImpl.class);
    }
}

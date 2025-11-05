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
package sleeper.datasource;

import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import sleeper.bulkimport.runner.StructTypeFactory;
import sleeper.clients.api.SleeperClient;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStore;

import java.util.Map;

/**
 * Doesn't need to be serialisable.
 */
public class SleeperTableProvider implements TableProvider, DataSourceRegister {
    private String instanceId;
    private String tableName;
    private StateStore stateStore;
    private SleeperClient sleeperClient;

    /**
     * A public, zero argument constuctor is required as this class implements TableProvider.
     */
    public SleeperTableProvider() {

    }

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        Map<String, String> optionsMap = options.asCaseSensitiveMap();
        instanceId = (String) optionsMap.get("instanceid");
        tableName = (String) optionsMap.get("tablename");
        loadSleeperClient();
        Schema schema = sleeperClient.getTableProperties(tableName).getSchema();
        stateStore = sleeperClient.getStateStore(tableName);
        return new StructTypeFactory().getStructType(schema);
    }

    @Override
    public Table getTable(StructType structType, Transform[] partitioning, Map<String, String> properties) {
        // String instanceId = (String) properties.get("instanceid");
        // String tableName = (String) properties.get("tablename");
        // SleeperClient sleeperClient = SleeperClient.createForInstanceId(instanceId);
        InstanceProperties instanceProperties = sleeperClient.getInstanceProperties();
        TableProperties tableProperties = sleeperClient.getTableProperties(tableName);
        Schema schema = tableProperties.getSchema();
        //  stateStore = sleeperClient.getStateStore(tableName);
        // PartitionSerDe partitionSerDe = new PartitionSerDe(schema);
        // List<String> partitionsAsJson = stateStore
        //         .getAllPartitions()
        //         .stream()
        //         .map(p -> partitionSerDe.toJson(p))
        //         .toList();
        // FileReferenceSerDe fileReferenceSerDe = new FileReferenceSerDe();
        // List<String> fileReferencesAsJson = stateStore
        //         .getFileReferences()
        //         .stream()
        //         .map(f -> fileReferenceSerDe.toJson(f))
        //         .toList();
        return new SleeperTable(instanceProperties, tableProperties, tableName, schema, structType, stateStore, stateStore.getAllPartitions(), stateStore.getFileReferences());
    }

    public boolean supportsExternalMetadata() {
        return true;
    }

    @Override
    public String shortName() {
        return "sleeper";
    }

    private void loadSleeperClient() {
        if (null == sleeperClient) {
            sleeperClient = SleeperClient.createForInstanceId(instanceId);
        }
    }
}

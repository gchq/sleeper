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
package sleeper.spark;

import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import sleeper.clients.api.SleeperClient;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.StateStore;
import sleeper.query.core.rowretrieval.QueryPlanner;

import java.util.Map;

/**
 * A TableProvider that allows a Sleeper table to be treated as a Spark table.
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
        return new StructTypeFactoryCopy().getStructType(schema);
    }

    @Override
    public Table getTable(StructType structType, Transform[] partitioning, Map<String, String> properties) {
        InstanceProperties instanceProperties = sleeperClient.getInstanceProperties();
        TableProperties tableProperties = sleeperClient.getTableProperties(tableName);
        QueryPlanner planner = new QueryPlanner(tableProperties, stateStore);
        return new SleeperTable(instanceProperties, tableProperties, structType, planner);
    }

    @Override
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

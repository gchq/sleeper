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

import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.query.core.rowretrieval.QueryPlanner;

import java.util.Set;

import static sleeper.core.properties.table.TableProperty.TABLE_NAME;

/**
 * Allows a Sleeper table to be used as a table in Spark which supports reading of the data.
 */
public class SleeperTable implements SupportsRead {
    private InstanceProperties instanceProperties;
    private TableProperties tableProperties;
    private StructType structType;
    private QueryPlanner queryPlanner;

    SleeperTable(InstanceProperties instanceProperties, TableProperties tableProperties, StructType structType,
            QueryPlanner queryPlanner) {
        this.instanceProperties = instanceProperties;
        this.tableProperties = tableProperties;
        this.structType = new StructTypeFactoryCopy().getStructType(tableProperties.getSchema());
        this.queryPlanner = queryPlanner;
        this.queryPlanner.init();
    }

    @Override
    public String name() {
        return tableProperties.get(TABLE_NAME);
    }

    @Override
    public StructType schema() {
        return structType;
    }

    @Override
    public Set<TableCapability> capabilities() {
        return Set.of(TableCapability.BATCH_READ);
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        return new SleeperScanBuilder(instanceProperties, tableProperties, structType, queryPlanner);
    }
}

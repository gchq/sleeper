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

import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import sleeper.core.partition.Partition;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.StateStore;

import java.util.List;
import java.util.Set;

/**
 * Doesn't need to be serialisable.
 *
 * TODO - why passing in statestore and partitions and filerefs?
 */
public class SleeperTable implements SupportsRead {
    // private String instancePropertiesAsString;
    // private String tablePropertiesAsString;
    private InstanceProperties instanceProperties;
    private TableProperties tableProperties;
    private String tableName;
    private Schema schema;
    private StructType structType;
    private StateStore stateStore;
    private List<Partition> partitions;
    private List<FileReference> fileReferences;

    SleeperTable(InstanceProperties instanceProperties, TableProperties tableProperties, String tableName, Schema schema,
            StructType structType, StateStore stateStore, List<Partition> partitions, List<FileReference> fileReferences) {
        // this.instancePropertiesAsString = instancePropertiesAsString;
        // this.tablePropertiesAsString = tablePropertiesAsString;
        this.instanceProperties = instanceProperties;
        this.tableProperties = tableProperties;
        this.tableName = tableName;
        this.schema = schema;
        this.structType = structType;
        this.stateStore = stateStore;
        this.partitions = partitions;
        this.fileReferences = fileReferences;
        // this.partitionsAsJson = partitionsAsJson;
        // this.fileReferencesAsJson = fileReferencesAsJson;
    }

    @Override
    public String name() {
        return tableName;
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
        return new SleeperScanBuilder(instanceProperties, tableProperties, schema, structType, stateStore, partitions, fileReferences);
    }
}
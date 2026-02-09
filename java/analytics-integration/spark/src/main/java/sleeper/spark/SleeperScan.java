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

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.query.core.rowretrieval.QueryPlanner;

/**
 * An implementation of Scan that provides a SleeperBatch.
 */
public class SleeperScan implements Scan {
    private InstanceProperties instanceProperties;
    private TableProperties tableProperties;
    private StructType structType;
    private QueryPlanner queryPlanner;
    private Filter[] pushedFilters;

    public SleeperScan(InstanceProperties instanceProperties, TableProperties tableProperties, StructType structType,
            QueryPlanner queryPlanner, Filter[] pushedFilters) {
        this.instanceProperties = instanceProperties;
        this.tableProperties = tableProperties;
        this.structType = structType;
        this.queryPlanner = queryPlanner;
        this.pushedFilters = pushedFilters;
    }

    @Override
    public StructType readSchema() {
        return structType;
    }

    @Override
    public Batch toBatch() {
        return new SleeperBatch(instanceProperties, tableProperties, queryPlanner, pushedFilters);
    }
}

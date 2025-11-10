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

import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.StateStore;

import java.util.ArrayList;
import java.util.List;

/**
 * Doesn't need to be serialisable.
 *
 * TODO Implement other filters.
 */
public class SleeperScanBuilder implements ScanBuilder, SupportsPushDownFilters {
    private InstanceProperties instanceProperties;
    private TableProperties tableProperties;
    private String keyFieldName;
    private StructType structType;
    private StateStore stateStore;
    // Initialise to an empty array because "It's possible that there is no filters in the query and
    // {@link #pushFilters(Filter[])} is never called, empty array should be returned for this case".
    private Filter[] pushedFilters = new Filter[0];

    public SleeperScanBuilder(InstanceProperties instanceProperties, TableProperties tableProperties, StructType structType,
            StateStore stateStore) {
        this.instanceProperties = instanceProperties;
        this.tableProperties = tableProperties;
        this.keyFieldName = tableProperties.getSchema().getRowKeyFieldNames().get(0);
        this.structType = structType;
        this.stateStore = stateStore;
    }

    @Override
    public Scan build() {
        return new SleeperScan(instanceProperties, tableProperties, structType, stateStore, pushedFilters);
    }

    /**
     * Returns the filters that need to be evaluated after scanning, i.e. those filters that cannot
     * be applied by the reader.
     */
    @Override
    public Filter[] pushFilters(Filter[] filters) {
        // Accept and remember pushdown filters on keyField to prune files
        List<Filter> accepted = new ArrayList<>();
        List<Filter> rejected = new ArrayList<>();
        for (Filter f : filters) {
            // Example: only accept equality filters on keyField
            if (f instanceof EqualTo && ((EqualTo) f).attribute().equals(keyFieldName)) {
                accepted.add(f);
            } else {
                rejected.add(f);
            }
        }
        pushedFilters = accepted.toArray(new Filter[0]);
        return rejected.toArray(new Filter[0]);
    }

    @Override
    public Filter[] pushedFilters() {
        return pushedFilters;
    }
}
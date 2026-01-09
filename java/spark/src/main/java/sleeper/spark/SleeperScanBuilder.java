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

import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.query.core.rowretrieval.QueryPlanner;
import sleeper.spark.FindFiltersToPush.PushedAndNonPushedFilters;

import java.util.Arrays;

/**
 * An implementation of ScanBuilder that allows a Scan to be created for a Sleeper table. This
 * {@link Scan} supports pushing down certain filters to the Sleeper readers.
 *
 * TODO Make this extend SupportsPushDownRequiredColumns so that column projections can be pushed down to the queries.
 */
public class SleeperScanBuilder implements SupportsPushDownFilters {
    private static final Logger LOGGER = LoggerFactory.getLogger(SleeperScanBuilder.class);

    private InstanceProperties instanceProperties;
    private TableProperties tableProperties;
    private Schema schema;
    private StructType structType;
    private QueryPlanner queryPlanner;
    // Initialise to an empty array because "It's possible that there is no filters in the query and
    // {@link #pushFilters(Filter[])} is never called, empty array should be returned for this case".
    private Filter[] pushedFilters = new Filter[0];

    public SleeperScanBuilder(InstanceProperties instanceProperties, TableProperties tableProperties, StructType structType,
            QueryPlanner queryPlanner) {
        this.instanceProperties = instanceProperties;
        this.tableProperties = tableProperties;
        this.schema = this.tableProperties.getSchema();
        this.structType = structType;
        this.queryPlanner = queryPlanner;
    }

    @Override
    public Scan build() {
        return new SleeperScan(instanceProperties, tableProperties, structType, queryPlanner, pushedFilters);
    }

    /**
     * Returns the filters that need to be evaluated after scanning, i.e. those filters that cannot
     * be applied by the reader.
     *
     * All of the filters in the array are to be anded together.
     *
     * @param filters The array of Filters to inspect
     */
    @Override
    public Filter[] pushFilters(Filter[] filters) {
        FindFiltersToPush findFiltersToPush = new FindFiltersToPush(schema);
        PushedAndNonPushedFilters pushedAndNonPushedFilters = findFiltersToPush.splitFiltersIntoPushedAndNonPushed(filters);
        LOGGER.debug("Received filters {}: the following filters were pushed {}, the following filters were not pushed {}",
                Arrays.toString(filters), pushedAndNonPushedFilters.getPushedFilters(), pushedAndNonPushedFilters.getNonPushedFilters());
        pushedFilters = pushedAndNonPushedFilters.getPushedFilters().toArray(new Filter[0]);
        return pushedAndNonPushedFilters.getNonPushedFilters().toArray(new Filter[0]);
    }

    @Override
    public Filter[] pushedFilters() {
        return pushedFilters;
    }
}

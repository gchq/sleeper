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
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.schema.Schema;
import sleeper.datasource.FindFiltersToPush.PushedAndNonPushedFilters;
import sleeper.query.core.rowretrieval.QueryPlanner;

/**
 * Doesn't need to be serialisable.
 *
 * TODO Make this extend SupportsPushDownRequiredColumns so that column projections can be pushed down to the queries.
 */
public class SleeperScanBuilder implements SupportsPushDownFilters {
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
     * Here are some examples of the Filter[] that is produced by WHERE clauses in certain queries:
     * - WHERE key = 'abc': EqualTo(key,abc) (Filter[] is of length 1)
     * - WHERE key > 'abc': GreaterThan(key,abc) (Filter[] is of length 1)
     * - WHERE key = 'abc' OR key = 'ghj': Or(EqualTo(key,abc),EqualTo(key,ghj)) (Filter[] is of length 1)
     * - WHERE key = 'abc' OR key = 'ghj' OR key = 'rst':
     * Or(Or(EqualTo(key,abc),EqualTo(key,ghj)),EqualTo(key,rst))(Filter[] is of length 1)
     * - WHERE key = 'a' OR key = 'b' OR key = 'c' OR key = 'd' OR key = 'e' OR key = 'f':
     * - Or(Or(Or(EqualTo(key,a),EqualTo(key,b)),EqualTo(key,c)),Or(Or(EqualTo(key,d),EqualTo(key,e)),EqualTo(key,f)))
     * - (Filter[] is of length 1)
     * - WHERE key1 = 'abc' AND key2 > 'x':
     * - Filter[] is of length 2
     * - EqualTo(key1,abc)
     * - GreaterThan(key2,x)
     * - WHERE (key1 = 'abc' OR key1 = 'ghj') AND key2 > 'x':
     * - Filter[] is of length 2
     * - Or(EqualTo(key,abc),EqualTo(key,qq))
     * - GreaterThan(value,x)
     *
     * @param filters The array of Filters to inspect
     */
    @Override
    public Filter[] pushFilters(Filter[] filters) {
        System.out.println("pushFilters method received: ");
        for (int i = 0; i < filters.length; i++) {
            System.out.println("\t" + filters[i]);
        }
        FindFiltersToPush findFiltersToPush = new FindFiltersToPush(schema);
        PushedAndNonPushedFilters pushedAndNonPushedFilters = findFiltersToPush.splitFiltersIntoPushedAndNonPushed(filters);
        pushedFilters = pushedAndNonPushedFilters.getPushedFilters().toArray(new Filter[0]);
        return pushedAndNonPushedFilters.getNonPushedFilters().toArray(new Filter[0]);
    }

    @Override
    public Filter[] pushedFilters() {
        return pushedFilters;
    }
}
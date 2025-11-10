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
import org.apache.spark.sql.sources.EqualNullSafe;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.GreaterThanOrEqual;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.sources.IsNotNull;
import org.apache.spark.sql.sources.IsNull;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.LessThanOrEqual;
import org.apache.spark.sql.sources.Not;
import org.apache.spark.sql.sources.StringContains;
import org.apache.spark.sql.sources.StringEndsWith;
import org.apache.spark.sql.sources.StringStartsWith;
import org.apache.spark.sql.types.StructType;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.query.core.rowretrieval.QueryPlanner;

import java.util.ArrayList;
import java.util.List;

/**
 * Doesn't need to be serialisable.
 *
 * TODO Implement other filters.
 *
 * TODO Make this extend SupportsPushDownRequiredColumns so that column projections can be pushed down to the queries.
 */
public class SleeperScanBuilder implements SupportsPushDownFilters {
    private InstanceProperties instanceProperties;
    private TableProperties tableProperties;
    private String keyFieldName;
    private StructType structType;
    private QueryPlanner queryPlanner;
    // Initialise to an empty array because "It's possible that there is no filters in the query and
    // {@link #pushFilters(Filter[])} is never called, empty array should be returned for this case".
    private Filter[] pushedFilters = new Filter[0];

    public SleeperScanBuilder(InstanceProperties instanceProperties, TableProperties tableProperties, StructType structType,
            QueryPlanner queryPlanner) {
        this.instanceProperties = instanceProperties;
        this.tableProperties = tableProperties;
        this.keyFieldName = tableProperties.getSchema().getRowKeyFieldNames().get(0);
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
     */
    @Override
    public Filter[] pushFilters(Filter[] filters) {
        // Accept and remember pushdown filters on keyField to prune files
        List<Filter> accepted = new ArrayList<>();
        List<Filter> rejected = new ArrayList<>();
        for (Filter f : filters) {
            if (acceptFilter(f)) {
                accepted.add(f);
            } else {
                rejected.add(f);
            }
        }
        pushedFilters = accepted.toArray(new Filter[0]);
        return rejected.toArray(new Filter[0]);
    }

    private boolean acceptFilter(Filter f) {
        // The list below includes all the filters that can be pushed down to data sourcees (taken from the source
        // code for Filter), and notes whether we can apply them or not. Note that in future we want to be able to
        // pass filters on queries down to the DataFusion egnine, when that is possible we will be able to accept
        // more of these filters.
        // - EqualTo                                 - apply if relates to a key column (see Filter docs for difference to EqualNullSafe)
        // - EqualNullSafe                           - apply if relates to a key column
        // - GreaterThan                             - apply if relates to a key column
        // - GreaterThanOrEqual                      - apply if relates to a key column
        // - LessThan                                - apply if relates to a key column
        // - LessThanOrEqual                         - apply if relates to a key column
        // - In (refers to value being in an array)  - apply if relates to a key column
        // - IsNull                                  - ignore
        // - IsNotNull                               - ignore
        // - And                                     - apply if the subterms are a filter we can apply
        // - Or                                      - apply if the subterms are a filter we can apply
        // - Not                                     - ignore
        // - StringStartsWith                        - apply if relates to a key column
        // - StringEndsWith                          - ignore
        // - StringContains                          - ignore
        if (f instanceof IsNull
                || f instanceof IsNotNull
                || f instanceof Not
                || f instanceof StringEndsWith
                || f instanceof StringContains) {
            return false;
        }
        if (f instanceof EqualTo && ((EqualTo) f).attribute().equals(keyFieldName)) {
            return true;
        }
        if (f instanceof EqualNullSafe && ((EqualNullSafe) f).attribute().equals(keyFieldName)) {
            return false; // TODO Apply this
        }
        if (f instanceof GreaterThan && ((GreaterThan) f).attribute().equals(keyFieldName)) {
            return false; // TODO Apply this
        }
        if (f instanceof GreaterThanOrEqual && ((GreaterThanOrEqual) f).attribute().equals(keyFieldName)) {
            return false; // TODO Apply this
        }
        if (f instanceof LessThan && ((LessThan) f).attribute().equals(keyFieldName)) {
            return false; // TODO Apply this
        }
        if (f instanceof LessThanOrEqual && ((LessThanOrEqual) f).attribute().equals(keyFieldName)) {
            return false; // TODO Apply this
        }
        if (f instanceof In && ((In) f).attribute().equals(keyFieldName)) {
            return false; // TODO Apply this
        }
        if (f instanceof StringStartsWith && ((StringStartsWith) f).attribute().equals(keyFieldName)) {
            return false; // TODO Apply this
        }
        return false;
    }

    @Override
    public Filter[] pushedFilters() {
        return pushedFilters;
    }
}
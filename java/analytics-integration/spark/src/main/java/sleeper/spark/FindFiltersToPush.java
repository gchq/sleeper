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

import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.GreaterThanOrEqual;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.LessThanOrEqual;
import org.apache.spark.sql.sources.Or;

import sleeper.core.schema.Schema;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Given an array of Filters provided by Spark, this class identifies which of those can be pushed
 * down to Sleeper, and which Spark will need to apply itself.
 */
public class FindFiltersToPush {
    private final Set<String> rowKeyFieldNames;

    public FindFiltersToPush(Schema schema) {
        this.rowKeyFieldNames = new HashSet<>(schema.getRowKeyFieldNames());
    }

    /**
     * Inspects the filters that Spark provides that apply the filtering the user wants to perform, and
     * splits them into two lists - the pushed and non-pushed filters.
     *
     * @param  filters the filters from the Spark query against the Sleeper DataFrame
     * @return         the pushed and non-pushed filters
     */
    public PushedAndNonPushedFilters splitFiltersIntoPushedAndNonPushed(Filter[] filters) {
        PushedAndNonPushedFilters pushedAndNonPushedFilters = new PushedAndNonPushedFilters();
        for (Filter filter : filters) {
            if (pushFilter(filter)) {
                pushedAndNonPushedFilters.addPushedFilter(filter);
            } else {
                pushedAndNonPushedFilters.addNonPushedFilter(filter);
            }
        }
        return pushedAndNonPushedFilters;
    }

    private boolean pushFilter(Filter filter) {
        if (filter instanceof EqualTo) {
            return rowKeyFieldNames.contains(((EqualTo) filter).attribute());
        } else if (filter instanceof GreaterThan) {
            return rowKeyFieldNames.contains(((GreaterThan) filter).attribute());
        } else if (filter instanceof GreaterThanOrEqual) {
            return rowKeyFieldNames.contains(((GreaterThanOrEqual) filter).attribute());
        } else if (filter instanceof LessThan) {
            return rowKeyFieldNames.contains(((LessThan) filter).attribute());
        } else if (filter instanceof LessThanOrEqual) {
            return rowKeyFieldNames.contains(((LessThanOrEqual) filter).attribute());
        } else if (filter instanceof In) {
            return rowKeyFieldNames.contains(((In) filter).attribute());
        } else if (filter instanceof Or) {
            Or or = (Or) filter;
            if (pushFilter(or.left()) && pushFilter(or.right())) {
                return true;
            }
        }
        return false;
    }

    /**
     * A class to store a list of filters that have been pushed to Sleeper and a list of those that have not.
     */
    public static class PushedAndNonPushedFilters {
        private List<Filter> pushedFilters;
        private List<Filter> nonPushedFilters;

        PushedAndNonPushedFilters() {
            this.pushedFilters = new ArrayList<>();
            this.nonPushedFilters = new ArrayList<>();
        }

        void addPushedFilter(Filter filter) {
            this.pushedFilters.add(filter);
        }

        void addNonPushedFilter(Filter filter) {
            this.nonPushedFilters.add(filter);
        }

        public List<Filter> getPushedFilters() {
            return pushedFilters;
        }

        public List<Filter> getNonPushedFilters() {
            return nonPushedFilters;
        }

        @Override
        public String toString() {
            return "PushedAndNonPushedFilters{pushedFilters=" + pushedFilters + ", nonPushedFilters=" + nonPushedFilters + "}";
        }
    }
}

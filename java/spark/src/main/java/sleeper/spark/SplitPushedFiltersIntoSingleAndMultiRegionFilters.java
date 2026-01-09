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
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.LessThanOrEqual;

import java.util.ArrayList;
import java.util.List;

/**
 * Splits an array of pushed filters into two sublists, one containing filters which correspond to a single
 * region, one containing filters which correspond to multiple regions.
 */
public class SplitPushedFiltersIntoSingleAndMultiRegionFilters {

    public SplitPushedFiltersIntoSingleAndMultiRegionFilters() {
    }

    /**
     * Takes the filters that have been pushed down to Sleeper and splits them into two types - those that
     * correspond to a single region and those that correspond to multiple regions.
     *
     * @param  filters The filters that have been pushed down to Sleeper
     * @return         a split of the pushed filters into ones which correspond to a single region and ones which
     *                 correspond to multiple regions
     */
    public SingleAndMultiRegionFilters splitPushedFilters(Filter[] filters) {
        SingleAndMultiRegionFilters singleAndMultiRegionFilters = new SingleAndMultiRegionFilters();
        for (Filter filter : filters) {
            if (singleRegionFilter(filter)) {
                singleAndMultiRegionFilters.addSingleRegionFilter(filter);
            } else {
                singleAndMultiRegionFilters.addMultiRegionFilter(filter);
            }
        }
        return singleAndMultiRegionFilters;
    }

    private boolean singleRegionFilter(Filter filter) {
        // We know that all filters are pushed filters, so we do not need to check whether they
        // refer to a key field.
        if (filter instanceof EqualTo
                || filter instanceof GreaterThan
                || filter instanceof GreaterThanOrEqual
                || filter instanceof LessThan
                || filter instanceof LessThanOrEqual) {
            return true;
        }
        return false;
    }

    /**
     * A class to store a list of filters corresponding to a single region and a list of filters corresponding
     * to multiple regions.
     */
    public static class SingleAndMultiRegionFilters {
        private List<Filter> singleRegionFilters;
        private List<Filter> multiRegionFilters;

        SingleAndMultiRegionFilters() {
            this.singleRegionFilters = new ArrayList<>();
            this.multiRegionFilters = new ArrayList<>();
        }

        void addSingleRegionFilter(Filter filter) {
            this.singleRegionFilters.add(filter);
        }

        void addMultiRegionFilter(Filter filter) {
            this.multiRegionFilters.add(filter);
        }

        public List<Filter> getSingleRegionFilters() {
            return singleRegionFilters;
        }

        public List<Filter> getMultiRegionFilters() {
            return multiRegionFilters;
        }
    }
}

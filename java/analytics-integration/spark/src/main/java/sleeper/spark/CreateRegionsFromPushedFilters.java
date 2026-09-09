/*
 * Copyright 2022-2026 Crown Copyright
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

import org.apache.spark.sql.sources.Filter;

import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.schema.Schema;
import sleeper.spark.SplitPushedFiltersIntoSingleAndMultiRegionFilters.SingleAndMultiRegionFilters;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Used to identify a list of the minimum Regions that the pushed filters correspond to.
 */
public class CreateRegionsFromPushedFilters {
    private final Schema schema;
    private final RangeFactory rangeFactory;

    public CreateRegionsFromPushedFilters(Schema schema) {
        this.schema = schema;
        this.rangeFactory = new RangeFactory(schema);
    }

    /**
     * Given the filters that have been pushed to Sleeper, identifies the minimum list of regions that need to
     * be read to return results corresponding to those filters. Note that this is not a one-to-one map between
     * the pushedFilters array and the list of regions.
     *
     * @param  pushedFilters the filters that have been pushed to Sleeper
     * @return               a list of the smallest regions that correspond to the filters
     */
    public List<Region> getMinimumRegionCoveringPushedFilters(Filter[] pushedFilters) {
        SplitPushedFiltersIntoSingleAndMultiRegionFilters split = new SplitPushedFiltersIntoSingleAndMultiRegionFilters();
        SingleAndMultiRegionFilters singleAndMultiRegionFilters = split.splitPushedFilters(pushedFilters);
        Region regionFromSingleRegionFilters = getRegionFromSingleRegionFilters(singleAndMultiRegionFilters.getSingleRegionFilters());
        // As the filters are combined with AND, each multi-region filter must be intersected with all the regions
        // found so far, i.e. the result is the cross product of the regions of the individual filters.
        List<Region> regions = List.of(regionFromSingleRegionFilters);
        for (Filter filter : singleAndMultiRegionFilters.getMultiRegionFilters()) {
            List<Region> regionsFromFilter = CreateRegionFromFilter.createRegionsFromFilter(filter, schema);
            List<Region> intersectedRegions = new ArrayList<>();
            for (Region regionSoFar : regions) {
                for (Region regionFromFilter : regionsFromFilter) {
                    Optional<Region> optionalIntersectedRegion = RegionIntersector.intersectRegions(regionSoFar, regionFromFilter, rangeFactory, schema);
                    if (optionalIntersectedRegion.isPresent()) {
                        intersectedRegions.add(optionalIntersectedRegion.get());
                    }
                }
            }
            regions = intersectedRegions;
        }
        return regions;
    }

    private Region getRegionFromSingleRegionFilters(List<Filter> singleRegionFilters) {
        if (singleRegionFilters == null || singleRegionFilters.isEmpty()) {
            return Region.coveringAllValuesOfAllRowKeys(schema);
        }
        Region intersectedRegion = CreateRegionFromFilter.createRegionFromSimpleFilter(singleRegionFilters.get(0), schema);
        for (int i = 1; i < singleRegionFilters.size(); i++) {
            Region region = CreateRegionFromFilter.createRegionFromSimpleFilter(singleRegionFilters.get(i), schema);
            intersectedRegion = RegionIntersector.intersectRegions(intersectedRegion, region, new RangeFactory(schema), schema).get();
        }
        return intersectedRegion;
    }
}

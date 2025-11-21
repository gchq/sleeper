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

import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.sources.Or;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.schema.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class CreateRegionsFromPushedFilters {
    private static final Logger LOGGER = LoggerFactory.getLogger(CreateRegionsFromPushedFilters.class);

    private final Schema schema;
    private final Filter[] pushedFilters;
    private final RangeFactory rangeFactory;

    public CreateRegionsFromPushedFilters(Schema schema, Filter[] pushedFilters) {
        this.schema = schema;
        this.pushedFilters = pushedFilters;
        this.rangeFactory = new RangeFactory(schema);
    }

    public static void findFiltersToPush(Schema schema, Filter[] filters, List<Filter> accepted, List<Filter> rejected) {
        for (Filter filter : filters) {
            Optional<List<Region>> optionalRegions = CreateRegionFromFilter.createRegionsFromFilter(filter, schema);
            if (!optionalRegions.isEmpty()) {
                System.out.println("Accepted filter " + filter);
                accepted.add(filter);
            } else {
                System.out.println("Rejected filter " + filter);
                rejected.add(filter);
            }
        }
    }

    void splitFiltersIntoAndsAndOrs(List<Filter> andFilters, List<Filter> orFilters) {
        for (Filter filter : pushedFilters) {
            if (!(filter instanceof Or) && !(filter instanceof In)) {
                andFilters.add(filter);
            } else {
                orFilters.add(filter);
            }
        }
    }

    public List<Filter> convertOrFiltersToListOfFilters(List<Filter> orFilters) {
        List<Filter> filters = new ArrayList<>();
        for (Filter filter : orFilters) {
            if (filter instanceof In) {
                In in = (In) filter;
                String fieldName = in.attribute();
                Object[] wantedValues = in.values();
                for (Object object : wantedValues) {
                    filters.add(new EqualTo(fieldName, object));
                }
            } else if (filter instanceof Or) {
                Or or = (Or) filter;
                filters.add(or.left());
                filters.add(or.right());
                // TODO - Need to deal with nested ORs
            }
        }
        return filters;
    }

    public List<Region> getMinimumRegionCoveringPushedFilters() {
        System.out.println("Pushed filters:");
        for (int i = 0; i < pushedFilters.length; i++) {
            System.out.println(pushedFilters[i]);
        }
        List<Filter> andFilters = new ArrayList<>();
        List<Filter> orFilters = new ArrayList<>();
        splitFiltersIntoAndsAndOrs(andFilters, orFilters);

        // Create a list of Regions from all the filters that are ANDed together
        List<Region> regionsFromAndFilters = new ArrayList<>();
        for (Filter filter : andFilters) {
            Optional<Region> optionalRegion = CreateRegionFromFilter.createRegionFromFilter(filter, schema);
            System.out.println("Created " + optionalRegion + " from filter " + filter);
            if (optionalRegion.isPresent()) {
                regionsFromAndFilters.add(optionalRegion.get());
            }
        }
        System.out.println("regionsFromAndFilters: " + regionsFromAndFilters);
        Optional<Region> intersectedAndRegion = RegionIntersector.intersectRegions(regionsFromAndFilters, rangeFactory, schema);
        System.out.println("intersectedAndRegion: " + intersectedAndRegion);

        // For each OR filter, create a new Region by adding it to the list of And Filters.
        List<Filter> orFiltersAList = convertOrFiltersToListOfFilters(orFilters);
        System.out.println("orFiltersAList: " + orFiltersAList);
        if (!orFiltersAList.isEmpty()) {
            List<Optional<Region>> finalRegions = new ArrayList<>();
            for (Filter filter : orFiltersAList) {
                Optional<Region> optionalRegionFromFilter = CreateRegionFromFilter.createRegionFromFilter(filter, schema);
                finalRegions.add(RegionIntersector.intersectOptionalRegions(intersectedAndRegion, optionalRegionFromFilter, rangeFactory, schema));
            }
            return finalRegions.stream().filter(Optional::isPresent).map(Optional::get).collect(Collectors.toList());
        }

        if (intersectedAndRegion.isEmpty()) {
            return List.of(Region.coveringAllValuesOfAllRowKeys(schema));
        }
        return List.of(intersectedAndRegion.get());
    }
}

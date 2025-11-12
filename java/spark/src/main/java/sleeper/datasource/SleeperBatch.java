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

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.GreaterThanOrEqual;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.LessThanOrEqual;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TableProperty;
import sleeper.core.range.Range;
import sleeper.core.range.Range.RangeFactory;
import sleeper.core.range.Region;
import sleeper.core.range.RegionSerDe;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.schema.SchemaSerDe;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.query.core.model.LeafPartitionQuery;
import sleeper.query.core.model.Query;
import sleeper.query.core.rowretrieval.QueryPlanner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Doesn't need to be serialisable.
 */
public class SleeperBatch implements Batch {
    private static final Logger LOGGER = LoggerFactory.getLogger(SleeperBatch.class);

    private InstanceProperties instanceProperties;
    private TableProperties tableProperties;
    private String tableId;
    private Schema schema;
    private String schemaAsJson;
    private RegionSerDe regionSerDe;
    private RangeFactory rangeFactory;
    private QueryPlanner queryPlanner;
    private Filter[] pushedFilters;

    public SleeperBatch(InstanceProperties instanceProperties, TableProperties tableProperties, QueryPlanner queryPlanner,
            Filter[] pushedFilters) {
        LOGGER.info("Created SleeperBatch");
        this.instanceProperties = instanceProperties;
        this.tableProperties = tableProperties;
        this.tableId = this.tableProperties.get(TableProperty.TABLE_ID);
        this.schema = this.tableProperties.getSchema();
        this.schemaAsJson = new SchemaSerDe().toJson(schema);
        this.regionSerDe = new RegionSerDe(this.tableProperties.getSchema());
        this.rangeFactory = new RangeFactory(schema);
        this.queryPlanner = queryPlanner;
        this.pushedFilters = pushedFilters;
    }

    @Override
    public InputPartition[] planInputPartitions() {
        List<Region> regions = getMinimumRegionCoveringPushedFilters();
        Query query = Query.builder()
                .queryId(UUID.randomUUID().toString())
                .tableName(tableProperties.get(TableProperty.TABLE_NAME))
                .regions(regions)
                .build();
        List<LeafPartitionQuery> leafPartitionQueries = queryPlanner.splitIntoLeafPartitionQueries(query);
        LOGGER.error("Split query into {} leaf partition queries", leafPartitionQueries.size());

        return leafPartitionQueries.stream()
                .map(q -> queryToSleeperInputPartition(q))
                .toArray(SleeperInputPartition[]::new);
    }

    private SleeperInputPartition queryToSleeperInputPartition(LeafPartitionQuery query) {
        String partitionRegionAsJson = regionSerDe.toJson(query.getPartitionRegion());
        List<String> regionsAsJson = query.getRegions()
                .stream()
                .map(r -> regionSerDe.toJson(r))
                .toList();
        return new SleeperInputPartition(tableId, schemaAsJson, query.getQueryId(), query.getSubQueryId(), query.getLeafPartitionId(),
                partitionRegionAsJson, regionsAsJson, query.getFiles());
    }

    /**
     * NB: Currently always returns a list containing one region. In future this may return
     * a list of regions as there are some combinations of filters that might result in
     * multiple regions.
     *
     * @return a list of the regions covering the pushed filters.
     */
    private List<Region> getMinimumRegionCoveringPushedFilters() {
        Schema schema = tableProperties.getSchema();

        // If no filters have been pushed to the data source then return a region covering the
        // entire key space.
        if (null == pushedFilters || pushedFilters.length == 0) {
            return List.of(Region.coveringAllValuesOfAllRowKeys(schema));
        }

        // pushedFilters contains all the filters that we can apply when reading the data.
        // Iterate through this array, identifying any which apply to a row-key field and are either =, <, <=, > or >=.
        // Group these by the row-key field. If any of the filters are ‘AND’ and one of the terms is also row-key
        // field and is either =, <, <=, > or >= then we can also extract that. In these grouped lists of filters,
        // look for one of the following combination of possibilities:
        // - key = ‘abc’
        // - key > ‘abc’
        // - key >= ‘abc’
        // - key < ‘abc’
        // - key <= ‘abc’
        // - ‘abc’ < key AND key < ‘def’
        // - 'abc’ < key AND key <= ‘def’
        // - 'abc’ <= key AND key < ‘def’
        // - 'abc’ <= key AND key <= ‘def’
        // This will let us create ranges for one or more of the row-key fields, and hence create a Region.
        Map<String, List<Filter>> rowKeyFieldNameToFilters = new HashMap<>();
        for (String rowKeyFieldName : schema.getRowKeyFieldNames()) {
            rowKeyFieldNameToFilters.put(rowKeyFieldName, new ArrayList<Filter>());
        }
        Set<String> rowKeyFieldNames = new HashSet<>(schema.getRowKeyFieldNames());
        int numFound = 0;
        for (Filter filter : pushedFilters) {
            String fieldName = filter.references()[0]; // TODO Is this safe?
            if (rowKeyFieldNames.contains(fieldName) && isFilterAnEqualityOrInequality(filter)) {
                rowKeyFieldNameToFilters.get(fieldName).add(filter);
                numFound++;
            }
        }
        if (numFound == 0) {
            return List.of(Region.coveringAllValuesOfAllRowKeys(schema));
        }
        Map<String, Range> rowKeyFieldNameToRange = new HashMap<>();
        for (Map.Entry<String, List<Filter>> entry : rowKeyFieldNameToFilters.entrySet()) {
            String rowKeyFieldName = entry.getKey();
            List<Filter> filters = entry.getValue();
            Range range;
            if (filters.size() == 1) {
                range = getRangeFromFilter(schema.getField(rowKeyFieldName).get(), filters.get(0));
            } else if (filters.size() == 2) {
                range = getRangeFromPairOfFilters(schema.getField(rowKeyFieldName).get(), filters.get(0), filters.get(1));
            } else {
                range = rangeFactory.createRangeCoveringAllValues(schema.getField(rowKeyFieldName).get());
            }
            rowKeyFieldNameToRange.put(rowKeyFieldName, range);
        }
        Region region = new Region(rowKeyFieldNameToRange.values());

        // In SleeperScanBuilder we only accepted filters of type EqualTo on the key column and we checked
        // that this was applied to the first key column. (In future we will accept more filters so the following
        // code will get more complex.)
        // EqualTo equalTo = (EqualTo) pushedFilters[0];
        // String key = (String) equalTo.value();
        // Range range = rangeFactory.createExactRange(schema.getRowKeyFields().get(0), key);
        // Region region = new Region(range);
        return List.of(region);
    }

    // This is technically unnecessary as the following types of filter are the only ones we accept at the
    // moment, but when we accept more filters we will need to identify these types.
    private boolean isFilterAnEqualityOrInequality(Filter filter) {
        if (filter instanceof EqualTo
                || filter instanceof GreaterThan
                || filter instanceof GreaterThanOrEqual
                || filter instanceof LessThan
                || filter instanceof LessThanOrEqual) {
            return true;
        }
        return false;
    }

    private Range getRangeFromFilter(Field field, Filter filter) {
        MutableRange mutableRange = new MutableRange(field);
        updateRangeWithFilter(mutableRange, filter);
        return mutableRange.getRange();
    }

    private Range getRangeFromPairOfFilters(Field field, Filter filter1, Filter filter2) {
        MutableRange mutableRange = new MutableRange(field);
        updateRangeWithFilter(mutableRange, filter1);
        updateRangeWithFilter(mutableRange, filter2);
        return mutableRange.getRange();
    }

    private class MutableRange {
        private final String fieldName;
        Object min;
        boolean minInclusive = false;
        Object max = null;
        boolean maxInclusive = false;

        MutableRange(Field field) {
            this.fieldName = field.getName();
            this.min = PrimitiveType.getMinimum(field.getType());
        }

        Range getRange() {
            return rangeFactory.createRange(fieldName, min, minInclusive, max, maxInclusive);
        }
    }

    private void updateRangeWithFilter(MutableRange mutableRange, Filter filter) {
        if (filter instanceof EqualTo) {
            // TODO - Can this happen? If filter1 is EqualTo and filter2 is a different filter of any type
            // then there can be no results.
            Object wantedKey = ((EqualTo) filter).value();
            mutableRange.minInclusive = true;
            mutableRange.min = wantedKey;
            mutableRange.maxInclusive = true;
            mutableRange.max = wantedKey;
        } else if (filter instanceof GreaterThan) {
            Object minimumKey = ((GreaterThan) filter).value();
            mutableRange.minInclusive = false;
            mutableRange.min = minimumKey;
        } else if (filter instanceof GreaterThanOrEqual) {
            Object minimumKey = ((GreaterThanOrEqual) filter).value();
            mutableRange.minInclusive = true;
            mutableRange.min = minimumKey;
        } else if (filter instanceof LessThan) {
            Object maxiumKey = ((LessThan) filter).value();
            mutableRange.maxInclusive = false;
            mutableRange.max = maxiumKey;
        } else if (filter instanceof LessThanOrEqual) {
            Object maxiumKey = ((LessThanOrEqual) filter).value();
            mutableRange.maxInclusive = true;
            mutableRange.max = maxiumKey;
        }
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new SleeperPartitionReaderFactory(Utils.saveInstancePropertiesToString(instanceProperties), Utils.saveTablePropertiesToString(tableProperties));
    }
}
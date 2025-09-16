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
package sleeper.query.core.rowretrieval;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.core.iterator.IteratorConfig;
import sleeper.core.iterator.IteratorCreationException;
import sleeper.core.iterator.IteratorFactory;
import sleeper.core.iterator.SortedRowIterator;
import sleeper.core.iterator.closeable.CloseableIterator;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.row.Row;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.util.ObjectFactory;
import sleeper.query.core.model.LeafPartitionQuery;
import sleeper.query.core.model.QueryException;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Executes a sub-query for a leaf partition, retrieving and processing rows.
 * This class orchestrates the retrieval of rows from a leaf partition
 * and applies compaction-time and query-time iterators.
 *
 */
public class LeafPartitionQueryExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(LeafPartitionQueryExecutor.class);

    private final ObjectFactory objectFactory;
    private final TableProperties tableProperties;
    private final LeafPartitionRowRetriever retriever;

    public LeafPartitionQueryExecutor(
            ObjectFactory objectFactory,
            TableProperties tableProperties,
            LeafPartitionRowRetriever retriever) {
        this.objectFactory = objectFactory;
        this.tableProperties = tableProperties;
        this.retriever = retriever;
    }

    /**
     * Retrieves the rows from a given LeafPartitionQuery.
     * This method initialises and applies both compaction-time and query-time iterators
     * to the retrieved rows before returning them.
     *
     * @param  leafPartitionQuery the Sleeper leaf partition query
     * @return                    the rows extracted by the query
     * @throws QueryException     if an exception occurred retrieving the rows from the query
     */
    public CloseableIterator<Row> getRows(LeafPartitionQuery leafPartitionQuery) throws QueryException {
        LOGGER.info("Retrieving rows for LeafPartitionQuery {}", leafPartitionQuery);
        Schema tableSchema = tableProperties.getSchema();
        SortedRowIterator compactionIterator;
        SortedRowIterator queryIterator;

        try {
            compactionIterator = createIterator(tableSchema, objectFactory, IteratorConfig.from(tableProperties));
            queryIterator = createIterator(tableSchema, objectFactory, IteratorConfig.builder()
                    .iteratorClassName(leafPartitionQuery.getQueryTimeIteratorClassName())
                    .iteratorConfigString(leafPartitionQuery.getQueryTimeIteratorConfig())
                    .filters(leafPartitionQuery.getQueryTimeFilters())
                    .aggregations(leafPartitionQuery.getQueryTimeAggregations(), tableSchema)
                    .build());
        } catch (IteratorCreationException e) {
            throw new QueryException("Failed to initialise iterators", e);
        }

        Schema dataReadSchema = createSchemaForDataRead(leafPartitionQuery, tableSchema, compactionIterator, queryIterator);

        try {
            CloseableIterator<Row> iterator = retriever.getRows(leafPartitionQuery, dataReadSchema);
            // Apply compaction time iterator
            if (null != compactionIterator) {
                iterator = compactionIterator.apply(iterator);
            }
            // Apply query time iterator
            if (null != queryIterator) {
                iterator = queryIterator.apply(iterator);
            }

            return iterator;
        } catch (RowRetrievalException e) {
            throw new QueryException("QueryException retrieving rows for LeafPartitionQuery", e);
        }
    }

    private Schema createSchemaForDataRead(LeafPartitionQuery query, Schema schema, SortedRowIterator compactionIterator, SortedRowIterator queryIterator) {
        List<String> requestedValueFields = query.getRequestedValueFields();
        if (requestedValueFields == null) {
            return schema;
        }

        Map<String, Field> fields = new HashMap<>();
        schema.getValueFields().forEach(field -> fields.put(field.getName(), field));

        Set<String> requiredFields = new HashSet<>(requestedValueFields);

        if (compactionIterator != null) {
            requiredFields.addAll(compactionIterator.getRequiredValueFields());
        }
        if (queryIterator != null) {
            requiredFields.addAll(queryIterator.getRequiredValueFields());
        }

        return Schema.builder()
                .rowKeyFields(schema.getRowKeyFields())
                .sortKeyFields(schema.getSortKeyFields())
                .valueFields(requiredFields.stream()
                        .map(fields::get)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList()))
                .build();
    }

    private SortedRowIterator createIterator(
            Schema schema,
            ObjectFactory objectFactory,
            IteratorConfig iteratorConfig) throws IteratorCreationException {
        if (iteratorConfig.shouldIteratorBeApplied()) {
            return new IteratorFactory(objectFactory)
                    .getIterator(iteratorConfig, schema);
        }
        return null;
    }
}

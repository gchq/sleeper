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
package sleeper.query.core.recordretrieval;

import sleeper.core.iterator.CloseableIterator;
import sleeper.core.row.Row;
import sleeper.core.schema.Schema;
import sleeper.query.core.model.LeafPartitionQuery;

/**
 * Retrieves rows in the regions and files specified by a sub query.
 */
@FunctionalInterface
public interface LeafPartitionRowRetriever {

    /**
     * Retrieves all rows in the regions and files specified by a sub query.
     *
     * @param  leafPartitionQuery       the sub query
     * @param  dataReadSchema           a schema containing all key fields for the table, and all value fields required
     *                                  for the query
     * @return                          An iterator over all rows in the specified files that are in the specified
     *                                  partition, and are in one of the specified regions. Only values specified in the
     *                                  data read schema will be returned. Other processing specified in the query will
     *                                  be applied by the caller.
     * @throws RecordRetrievalException if the first row of any file could not be read
     */
    CloseableIterator<Row> getRows(LeafPartitionQuery leafPartitionQuery, Schema dataReadSchema) throws RecordRetrievalException;
}

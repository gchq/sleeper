/*
 * Copyright 2022-2024 Crown Copyright
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
package sleeper.query.core;

import sleeper.core.iterator.CloseableIterator;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.query.core.model.LeafPartitionQuery;

@FunctionalInterface
public interface LeafPartitionRecordRetriever {

    /**
     * Retrieve all records in the regions and files specified by a sub query.
     *
     * @param  leafPartitionQuery       the sub query
     * @param  dataReadSchema           a schema containing all key fields for the table, and all value fields required
     *                                  for the query
     * @return                          An iterator over all records in the specified files that are in the specified
     *                                  partition, and are in one of the specified regions. Only values specified in the
     *                                  data read schema will be returned. Other processing specified in the query will
     *                                  be applied by the caller.
     * @throws RecordRetrievalException Thrown if the first record of any file could not be read.
     */
    CloseableIterator<Record> getRecords(LeafPartitionQuery leafPartitionQuery, Schema dataReadSchema) throws RecordRetrievalException;
}

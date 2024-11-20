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
package sleeper.ingest.runner.impl.recordbatch;

import sleeper.core.iterator.CloseableIterator;
import sleeper.core.record.Record;

import java.io.IOException;

/**
 * A single-use batch of data, where the data is supplied in any format and is retrieved in sorted order. Converts the
 * source data to an iterator of Sleeper {@link Record} objects. Data may be supplied in any format and it is the
 * responsibility of implementing classes to handle any type conversion.
 *
 * @param <INCOMINGDATATYPE> The type of data that this record batch accepts
 */
public interface RecordBatch<INCOMINGDATATYPE> extends AutoCloseable {
    /**
     * Append data to the batch.
     *
     * @param  data        the data to append
     * @throws IOException if there was a failure writing to the file
     */
    void append(INCOMINGDATATYPE data) throws IOException;

    /**
     * Indicate whether the batch has any avaialble space.
     *
     * @return true if the batch is full
     */
    boolean isFull();

    /**
     * Generate an iterator which returns the records in sort-order. It is the responsibility of the
     * caller to close the iterator. This method may only be called once per {@link RecordBatch}.
     *
     * @return             the iterator
     * @throws IOException if there was a failure writing the file
     */
    CloseableIterator<Record> createOrderedRecordIterator() throws IOException;

    /**
     * Close the batch, freeing all memory, clearing temporary disk and other resources.
     */
    @Override
    void close();
}

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
package sleeper.core.row.serialiser;

import sleeper.core.row.ResultsBatch;

/**
 * Serialises and deserialises batches of results for a query.
 */
public interface ResultsBatchSerialiser {

    /**
     * Serialise a results batch to a string.
     *
     * @param  resultsBatch the results batch
     * @return              a serialised string
     */
    String serialise(ResultsBatch resultsBatch);

    /**
     * Deserialise a results batch from a string.
     *
     * @param  serialisedResultsBatch the serialised results batch
     * @return                        a {@link ResultsBatch} object
     */
    ResultsBatch deserialise(String serialisedResultsBatch);
}

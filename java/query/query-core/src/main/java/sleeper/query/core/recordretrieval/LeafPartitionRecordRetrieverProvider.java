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

import sleeper.core.properties.table.TableProperties;

/**
 * Creates record retrievers to read underlying data files.
 */
@FunctionalInterface
public interface LeafPartitionRecordRetrieverProvider {

    /**
     * Creates a record retriever to read files in a given Sleeper table.
     *
     * @param  tableProperties the Sleeper table properties
     * @return                 the record retriever
     */
    LeafPartitionRecordRetriever getRecordRetriever(TableProperties tableProperties);

}

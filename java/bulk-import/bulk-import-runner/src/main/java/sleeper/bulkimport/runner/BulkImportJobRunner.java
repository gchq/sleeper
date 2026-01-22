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

package sleeper.bulkimport.runner;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import sleeper.bulkimport.runner.common.SparkFileReferenceRow;
import sleeper.core.statestore.StateStore;

import java.io.IOException;
import java.util.stream.Collectors;

/**
 * A runner to split Sleeper rows across partitions and write the data to files. This takes in a {@link Dataset} of
 * {@link Row}s which each contain a Sleeper row. It should split the rows into partitions, write each partition's
 * data to a file in S3, and return a {@link Dataset} of {@link Row}s containing metadata for each file. Those rows must
 * contain fields as specified in {@link SparkFileReferenceRow}. These will then be used to update the Sleeper
 * {@link StateStore}.
 */
@FunctionalInterface
public interface BulkImportJobRunner {
    Dataset<Row> createFileReferences(BulkImportSparkContext input) throws IOException;

    default BulkImportJobDriver.BulkImporter<BulkImportSparkContext> asImporter() {
        return context -> createFileReferences(context)
                .collectAsList().stream()
                .map(SparkFileReferenceRow::createFileReference)
                .collect(Collectors.toList());
    }
}

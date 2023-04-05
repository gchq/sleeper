/*
 * Copyright 2022-2023 Crown Copyright
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
package sleeper.bulkimport.job.runner.dataframelocalsort;

import sleeper.bulkimport.job.runner.BulkImportJobRunner;

/**
 * The {@link BulkImportDataframeLocalSortRunner} is a {@link BulkImportJobRunner} which
 * uses Spark's Dataframe API to efficiently sort and write out the data split by
 * Sleeoer partition.
 */
public class BulkImportDataframeLocalSortRunner extends BulkImportJobRunner {

    public BulkImportDataframeLocalSortRunner() {
        super(new BulkImportDataframeLocalSortPartitioner());
    }

    public static void main(String[] args) throws Exception {
        BulkImportJobRunner.start(args, new BulkImportDataframeLocalSortPartitioner());
    }
}

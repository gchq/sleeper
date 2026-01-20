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

import sleeper.bulkimport.core.job.BulkImportJob;
import sleeper.bulkimport.runner.common.SparkFileReferenceRow;
import sleeper.core.partition.Partition;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.statestore.FileReference;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class BulkImportSparkSessionRunner
        implements BulkImportJobDriver.BulkImporter<BulkImportSparkContext>, BulkImportJobDriver.ContextCreator<BulkImportSparkContext> {

    private final BulkImportJobRunner jobRunner;
    private final InstanceProperties instanceProperties;

    public BulkImportSparkSessionRunner(
            BulkImportJobRunner jobRunner, InstanceProperties instanceProperties) {
        this.jobRunner = jobRunner;
        this.instanceProperties = instanceProperties;
    }

    @Override
    public BulkImportSparkContext createContext(TableProperties tableProperties, List<Partition> allPartitions, BulkImportJob job) {
        return BulkImportSparkContext.create(instanceProperties, tableProperties, allPartitions, job.getFiles());
    }

    @Override
    public List<FileReference> createFileReferences(BulkImportSparkContext context) throws IOException {
        return jobRunner.createFileReferences(context)
                .collectAsList().stream()
                .map(SparkFileReferenceRow::createFileReference)
                .collect(Collectors.toList());
    }
}

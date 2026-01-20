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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.bulkimport.core.job.BulkImportJob;
import sleeper.bulkimport.runner.common.SparkFileReferenceRow;
import sleeper.core.partition.Partition;
import sleeper.core.properties.instance.InstanceProperties;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.properties.table.TablePropertiesProvider;
import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.StateStoreProvider;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class BulkImportSparkSessionRunner
        implements BulkImportJobDriver.SessionRunner, BulkImportJobDriver.SessionRunnerNew<BulkImportSparkContext>, BulkImportJobDriver.ContextCreator<BulkImportSparkContext> {
    private static final Logger LOGGER = LoggerFactory.getLogger(BulkImportSparkSessionRunner.class);

    private final BulkImportJobRunner jobRunner;
    private final InstanceProperties instanceProperties;
    private final TablePropertiesProvider tablePropertiesProvider;
    private final StateStoreProvider stateStoreProvider;

    public BulkImportSparkSessionRunner(
            BulkImportJobRunner jobRunner, InstanceProperties instanceProperties,
            TablePropertiesProvider tablePropertiesProvider, StateStoreProvider stateStoreProvider) {
        this.jobRunner = jobRunner;
        this.instanceProperties = instanceProperties;
        this.tablePropertiesProvider = tablePropertiesProvider;
        this.stateStoreProvider = stateStoreProvider;
    }

    @Override
    public BulkImportJobOutput run(BulkImportJob job) throws IOException {
        LOGGER.info("Loading table properties and schema for table {}", job.getTableName());
        TableProperties tableProperties = tablePropertiesProvider.getByName(job.getTableName());
        LOGGER.info("Loading partitions");
        List<Partition> allPartitions = stateStoreProvider.getStateStore(tableProperties).getAllPartitions();

        LOGGER.info("Running bulk import job with id {}", job.getId());
        BulkImportSparkContext context = createContext(tableProperties, allPartitions, job);
        List<FileReference> fileReferences = createFileReferences(context);

        return new BulkImportJobOutput(fileReferences, context::stopSparkContext);
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

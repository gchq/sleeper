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
package sleeper.compaction.job;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.statestore.FileReference;

import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

import static java.util.stream.Collectors.toList;
import static sleeper.configuration.properties.table.TableProperty.ITERATOR_CLASS_NAME;
import static sleeper.configuration.properties.table.TableProperty.ITERATOR_CONFIG;
import static sleeper.configuration.properties.table.TableProperty.TABLE_ID;

public class CompactionJobFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(CompactionJobFactory.class);

    private final String tableId;
    private final CompactionOutputFileNameFactory fileNameFactory;
    private final String iteratorClassName;
    private final String iteratorConfig;
    private final Supplier<String> jobIdSupplier;

    public CompactionJobFactory(InstanceProperties instanceProperties, TableProperties tableProperties) {
        this(instanceProperties, tableProperties, () -> UUID.randomUUID().toString());
    }

    public CompactionJobFactory(InstanceProperties instanceProperties, TableProperties tableProperties, Supplier<String> jobIdSupplier) {
        tableId = tableProperties.get(TABLE_ID);
        fileNameFactory = CompactionOutputFileNameFactory.forTable(instanceProperties, tableProperties);
        iteratorClassName = tableProperties.get(ITERATOR_CLASS_NAME);
        iteratorConfig = tableProperties.get(ITERATOR_CONFIG);
        this.jobIdSupplier = jobIdSupplier;
        LOGGER.info("Initialised CompactionFactory with table {}, filename prefix {}",
                tableProperties.getStatus(), fileNameFactory.getOutputFilePrefix());
    }

    public CompactionJob createCompactionJob(
            List<FileReference> files, String partition) {
        return createCompactionJob(jobIdSupplier.get(), files, partition);
    }

    public CompactionJob createCompactionJob(
            String jobId, List<FileReference> files, String partition) {
        for (FileReference fileReference : files) {
            if (!partition.equals(fileReference.getPartitionId())) {
                throw new IllegalArgumentException("Found file with partition which is different to the provided partition (partition = "
                        + partition + ", FileReference = " + fileReference);
            }
        }
        return createCompactionJobWithFilenames(jobId,
                files.stream().map(FileReference::getFilename).collect(toList()),
                partition);
    }

    public CompactionJob createCompactionJobWithFilenames(
            String jobId, List<String> filenames, String partition) {
        CompactionJob job = createCompactionJobBuilder(jobId, filenames, partition).build();

        LOGGER.info("Created compaction job of id {} to compact {} files in partition {} to output file {}",
                job.getId(), filenames.size(), partition, job.getOutputFile());

        return job;
    }

    private CompactionJob.Builder createCompactionJobBuilder(String jobId, List<String> filenames, String partition) {
        String outputFile = fileNameFactory.jobPartitionFile(jobId, partition);
        return CompactionJob.builder()
                .tableId(tableId)
                .jobId(jobId)
                .inputFiles(filenames)
                .outputFile(outputFile)
                .partitionId(partition)
                .iteratorClassName(iteratorClassName)
                .iteratorConfig(iteratorConfig);
    }
}

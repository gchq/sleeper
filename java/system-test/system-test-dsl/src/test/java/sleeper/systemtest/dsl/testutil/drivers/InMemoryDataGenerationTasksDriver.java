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
package sleeper.systemtest.dsl.testutil.drivers;

import sleeper.core.iterator.IteratorCreationException;
import sleeper.core.properties.table.TableProperties;
import sleeper.core.record.testutils.InMemoryRecordStore;
import sleeper.core.row.Record;
import sleeper.core.util.PollWithRetries;
import sleeper.ingest.runner.impl.IngestCoordinator;
import sleeper.ingest.runner.testutils.InMemoryIngest;
import sleeper.sketches.testutils.InMemorySketchesStore;
import sleeper.systemtest.configuration.SystemTestDataGenerationJob;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;
import sleeper.systemtest.dsl.sourcedata.DataGenerationTasksDriver;
import sleeper.systemtest.dsl.sourcedata.GenerateNumberedRecords;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class InMemoryDataGenerationTasksDriver implements DataGenerationTasksDriver {

    private final SystemTestInstanceContext instance;
    private final InMemoryRecordStore data;
    private final InMemorySketchesStore sketches;
    private final Random random = new Random(0);

    public InMemoryDataGenerationTasksDriver(SystemTestInstanceContext instance, InMemoryRecordStore data, InMemorySketchesStore sketches) {
        this.instance = instance;
        this.data = data;
        this.sketches = sketches;
    }

    @Override
    public void runDataGenerationJobs(List<SystemTestDataGenerationJob> jobs, PollWithRetries poll) {
        for (SystemTestDataGenerationJob job : jobs) {
            TableProperties tableProperties = instance.getTablePropertiesByDeployedName(job.getTableName()).orElseThrow();
            for (int i = 0; i < job.getNumberOfIngests(); i++) {
                try (IngestCoordinator<Record> coordinator = ingest(tableProperties).createCoordinator()) {
                    for (Record record : generateRecords(job, tableProperties)) {
                        coordinator.write(record);
                    }
                } catch (IteratorCreationException e) {
                    throw new RuntimeException(e);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }
    }

    private InMemoryIngest ingest(TableProperties tableProperties) {
        return new InMemoryIngest(
                instance.getInstanceProperties(), tableProperties,
                instance.getStateStore(tableProperties),
                data, sketches);
    }

    private Iterable<Record> generateRecords(SystemTestDataGenerationJob job, TableProperties tableProperties) {
        List<Long> recordNumbers = LongStream.range(0, job.getRecordsPerIngest())
                .mapToObj(num -> num)
                .collect(Collectors.toList());
        Collections.shuffle(recordNumbers, random);
        GenerateNumberedRecords generator = instance.numberedRecords(tableProperties.getSchema());
        return () -> recordNumbers.stream().map(generator::generateRecord).iterator();
    }

}

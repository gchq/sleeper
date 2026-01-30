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
import sleeper.core.row.Row;
import sleeper.core.row.testutils.InMemoryRowStore;
import sleeper.core.util.PollWithRetries;
import sleeper.ingest.runner.impl.IngestCoordinator;
import sleeper.ingest.runner.testutils.InMemoryIngest;
import sleeper.sketches.testutils.InMemorySketchesStore;
import sleeper.systemtest.configuration.SystemTestDataGenerationJob;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;
import sleeper.systemtest.dsl.sourcedata.DataGenerationTasksDriver;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class InMemoryDataGenerationTasksDriver implements DataGenerationTasksDriver {

    private final SystemTestInstanceContext instance;
    private final InMemoryRowStore data;
    private final InMemorySketchesStore sketches;
    private final Random random = new Random(0);

    public InMemoryDataGenerationTasksDriver(SystemTestInstanceContext instance, InMemoryRowStore data, InMemorySketchesStore sketches) {
        this.instance = instance;
        this.data = data;
        this.sketches = sketches;
    }

    @Override
    public void runDataGenerationJobs(int numberOfJobs, SystemTestDataGenerationJob jobSpec, PollWithRetries poll) {
        for (int i = 0; i < numberOfJobs; i++) {
            TableProperties tableProperties = instance.getTablePropertiesByDeployedName(jobSpec.getTableName()).orElseThrow();
            for (int j = 0; j < jobSpec.getNumberOfIngests(); j++) {
                try (IngestCoordinator<Row> coordinator = ingest(tableProperties).createCoordinator()) {
                    for (Row row : generateRows(jobSpec, tableProperties)) {
                        coordinator.write(row);
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

    private Iterable<Row> generateRows(SystemTestDataGenerationJob job, TableProperties tableProperties) {
        List<Long> rowNumbers = LongStream.range(0, job.getRowsPerIngest())
                .mapToObj(num -> num)
                .collect(Collectors.toList());
        Collections.shuffle(rowNumbers, random);
        return instance.numberedRows(tableProperties.getSchema())
                .iterableOver(rowNumbers);
    }

}

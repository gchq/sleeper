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

package sleeper.systemtest.suite.dsl;

import sleeper.core.record.Record;
import sleeper.systemtest.datageneration.GenerateNumberedRecords;
import sleeper.systemtest.drivers.ingest.IngestSourceFilesContext;
import sleeper.systemtest.drivers.instance.SleeperInstanceContext;

import java.util.stream.LongStream;
import java.util.stream.Stream;

public class SystemTestSourceFiles {
    private final SleeperInstanceContext instanceContext;
    private final IngestSourceFilesContext sourceFilesContext;

    public SystemTestSourceFiles(SleeperInstanceContext instanceContext, IngestSourceFilesContext sourceFilesContext) {
        this.instanceContext = instanceContext;
        this.sourceFilesContext = sourceFilesContext;
    }

    public SystemTestSourceFiles createWithNumberedRecords(String filename, LongStream numbers) {
        return create(filename, GenerateNumberedRecords.from(instanceContext.getTableProperties().getSchema(), numbers));
    }

    public SystemTestSourceFiles create(String filename, Record... records) {
        return create(filename, Stream.of(records));
    }

    private SystemTestSourceFiles create(String filename, Stream<Record> records) {
        sourceFilesContext.writeFile(instanceContext.getTableProperties(), filename, records.iterator());
        return this;
    }
}

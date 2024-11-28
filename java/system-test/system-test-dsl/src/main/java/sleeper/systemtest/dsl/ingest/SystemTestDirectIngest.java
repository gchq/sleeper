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

package sleeper.systemtest.dsl.ingest;

import sleeper.core.record.Record;
import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.LongStream;

public class SystemTestDirectIngest {

    private final SystemTestInstanceContext instance;
    private final DirectIngestDriver driver;
    private final Path tempDir;

    public SystemTestDirectIngest(SystemTestInstanceContext instance, DirectIngestDriver driver, Path tempDir) {
        this.instance = instance;
        this.driver = driver;
        this.tempDir = tempDir;
    }

    public SystemTestDirectIngest numberedRecords(LongStream numbers) {
        driver.ingest(tempDir, instance.numberedRecords().iteratorFrom(numbers));
        return this;
    }

    public void records(Record... records) {
        driver.ingest(tempDir, List.of(records).iterator());
    }
}

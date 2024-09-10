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

package sleeper.systemtest.dsl.sourcedata;

import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;

import java.nio.file.Path;
import java.util.stream.LongStream;

public class SystemTestLocalFiles {
    private final SystemTestInstanceContext instance;
    private final IngestLocalFilesDriver driver;
    private final Path tempDir;

    public SystemTestLocalFiles(SystemTestInstanceContext instance, IngestLocalFilesDriver driver, Path tempDir) {
        this.instance = instance;
        this.driver = driver;
        this.tempDir = tempDir;
    }

    public void createWithNumberedRecords(String file, LongStream numbers) {
        driver.writeFile(
                instance.getTableProperties(),
                tempDir.resolve(file),
                instance.generateNumberedRecords(numbers).iterator());
    }
}

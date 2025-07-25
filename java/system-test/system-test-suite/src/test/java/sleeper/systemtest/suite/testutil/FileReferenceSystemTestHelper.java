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

package sleeper.systemtest.suite.testutil;

import sleeper.core.statestore.FileReference;
import sleeper.core.statestore.FileReferenceFactory;
import sleeper.systemtest.dsl.SleeperSystemTest;

import java.util.List;

public class FileReferenceSystemTestHelper {

    private FileReferenceSystemTestHelper() {
    }

    public static FileReferenceFactory fileFactory(SleeperSystemTest sleeper) {
        return FileReferenceFactory.from(sleeper.instanceProperties(), sleeper.tableProperties(), sleeper.partitioning().tree());
    }

    public static long numberOfRecordsIn(List<? extends FileReference> files) {
        return files.stream().mapToLong(FileReference::getNumberOfRows).sum();
    }
}

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

package sleeper.systemtest.suite.testutil;

import sleeper.core.statestore.FileInfo;
import sleeper.core.statestore.FileInfoFactory;
import sleeper.systemtest.suite.dsl.SleeperSystemTest;

import java.util.List;

public class FileInfoSystemTestHelper {

    private FileInfoSystemTestHelper() {
    }

    public static FileInfoFactory fileInfoFactory(SleeperSystemTest sleeper) {
        return new FileInfoFactory(sleeper.tableProperties().getSchema(), sleeper.stateStore().allPartitions());
    }

    public static long numberOfRecordsIn(List<? extends FileInfo> files) {
        return files.stream().mapToLong(FileInfo::getNumberOfRecords).sum();
    }
}

/*
 * Copyright 2022 Crown Copyright
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
package sleeper.compaction.jobexecution.testutils;

import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import sleeper.compaction.job.CompactionJobFactory;
import sleeper.core.CommonTestConstants;

public class CompactSortedFilesTestBase {
    public static final String DEFAULT_TASK_ID = "task-id";
    @Rule
    public TemporaryFolder folder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);
    protected String folderName;

    @Before
    public void setUpBase() throws Exception {
        folderName = folder.newFolder().getAbsolutePath();
    }

    protected CompactionJobFactory compactionFactory() {
        return compactionFactoryBuilder().build();
    }

    protected CompactionJobFactory.Builder compactionFactoryBuilder() {
        return CompactionJobFactory.withTableName("table").outputFilePrefix(folderName);
    }
}

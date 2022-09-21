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
package sleeper.compaction.jobexecution;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import sleeper.compaction.job.CompactionFactory;
import sleeper.compaction.job.CompactionJob;
import sleeper.core.CommonTestConstants;
import sleeper.core.record.Record;
import sleeper.core.schema.Schema;
import sleeper.core.schema.type.LongType;
import sleeper.statestore.StateStore;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static sleeper.compaction.jobexecution.CompactSortedFilesTestData.combineSortedBySingleKey;
import static sleeper.compaction.jobexecution.CompactSortedFilesTestData.keyAndTwoValuesSortedEvenLongs;
import static sleeper.compaction.jobexecution.CompactSortedFilesTestData.keyAndTwoValuesSortedOddLongs;
import static sleeper.compaction.jobexecution.CompactSortedFilesTestData.readDataFile;
import static sleeper.compaction.jobexecution.CompactSortedFilesTestUtils.assertReadyForGC;
import static sleeper.compaction.jobexecution.CompactSortedFilesTestUtils.createCompactSortedFiles;
import static sleeper.compaction.jobexecution.CompactSortedFilesTestUtils.createSchemaWithTypesForKeyAndTwoValues;

public class CompactSortedFilesTest {

    private final StateStore stateStore = mock(StateStore.class);

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);
    private String folderName;

    @Before
    public void setUp() throws Exception {
        folderName = folder.newFolder().getAbsolutePath();
    }

    private CompactionFactory compactionFactory() {
        return compactionFactoryBuilder().build();
    }

    private CompactionFactory.Builder compactionFactoryBuilder() {
        return CompactionFactory.withTableName("table").outputFilePrefix(folderName);
    }

    @Test
    @Ignore("Need to use alternative state store or set up mock")
    public void filesShouldMergeCorrectlyAndDynamoUpdatedLongKey() throws Exception {
        // Given
        Schema schema = createSchemaWithTypesForKeyAndTwoValues(new LongType(), new LongType(), new LongType());
        CompactSortedFilesTestDataHelper dataHelper = new CompactSortedFilesTestDataHelper(schema, stateStore);

        List<Record> data1 = keyAndTwoValuesSortedEvenLongs();
        List<Record> data2 = keyAndTwoValuesSortedOddLongs();
        dataHelper.writeLeafFile(folderName + "/file1.parquet", data1, 0L, 198L);
        dataHelper.writeLeafFile(folderName + "/file2.parquet", data2, 1L, 199L);

        CompactionJob compactionJob = compactionFactory().createCompactionJob(
                dataHelper.allFileInfos(), dataHelper.singlePartition().getId());
        dataHelper.addFilesToStateStoreForJob(compactionJob);

        // When
        CompactSortedFiles compactSortedFiles = createCompactSortedFiles(schema, compactionJob, stateStore);
        CompactSortedFiles.CompactionJobSummary summary = compactSortedFiles.compact();

        // Then
        //  - Read output file and check that it contains the right results
        List<Record> expectedResults = combineSortedBySingleKey(data1, data2);
        assertThat(summary.getLinesRead()).isEqualTo(expectedResults.size());
        assertThat(summary.getLinesWritten()).isEqualTo(expectedResults.size());
        assertThat(readDataFile(schema, compactionJob.getOutputFile())).isEqualTo(expectedResults);

        // - Check DynamoDBStateStore has correct ready for GC files
        assertReadyForGC(stateStore, dataHelper.allFileInfos());

        // - Check DynamoDBStateStore has correct active files
        assertThat(stateStore.getActiveFiles())
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("lastStateStoreUpdateTime")
                .containsExactly(dataHelper.expectedLeafFile(compactionJob.getOutputFile(), 200L, 0L, 199L));
    }
}

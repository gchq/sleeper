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

package sleeper.systemtest.suite;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import sleeper.cdk.stack.bulkimport.PersistentEmrBulkImportStack;
import sleeper.systemtest.suite.dsl.SleeperSystemTest;
import sleeper.systemtest.suite.fixtures.SystemTestSchema;
import sleeper.systemtest.suite.testutil.ReportingExtension;

import java.io.IOException;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.instance.SystemDefinedInstanceProperty.BULK_IMPORT_PERSISTENT_EMR_JOB_QUEUE_URL;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_MIN_LEAF_PARTITION_COUNT;
import static sleeper.systemtest.datageneration.GenerateNumberedValue.stringFromPrefixAndPadToSize;
import static sleeper.systemtest.datageneration.GenerateNumberedValueOverrides.overrideField;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.MAIN;
import static sleeper.systemtest.suite.testutil.PartitionsTestHelper.partitionsBuilder;

@Tag("SystemTest")
public class EmrPersistentBulkImportIT {
    private final SleeperSystemTest sleeper = SleeperSystemTest.getInstance();

    @RegisterExtension
    public final ReportingExtension reporting = ReportingExtension.reportAlways(
            sleeper.reportsForExtension().ingestJobs());

    @BeforeEach
    void setUp() throws IOException, InterruptedException {
        sleeper.connectToInstance(MAIN);
        sleeper.enableOptionalStack(PersistentEmrBulkImportStack.class);
    }

    @AfterEach
    void tearDown() throws IOException, InterruptedException {
        sleeper.disableOptionalStack(PersistentEmrBulkImportStack.class);
    }

    @Test
    void shouldBulkImport100Records() throws InterruptedException {
        // Given
        sleeper.updateTableProperties(properties ->
                properties.set(BULK_IMPORT_MIN_LEAF_PARTITION_COUNT, "0"));
        sleeper.partitioning().setPartitions(partitionsBuilder(sleeper)
                .rootFirst("root")
                .splitToNewChildren("root", "A", "B", "row-50")
                .buildTree());
        sleeper.setGeneratorOverrides(overrideField(
                SystemTestSchema.ROW_KEY_FIELD_NAME,
                stringFromPrefixAndPadToSize("row-", 2)));
        sleeper.sourceFiles().createWithNumberedRecords("test.parquet", LongStream.range(0, 100));

        // When
        sleeper.ingest().byQueue()
                .sendSourceFiles(BULK_IMPORT_PERSISTENT_EMR_JOB_QUEUE_URL, "test.parquet")
                .waitForJobs();

        // Then
        assertThat(sleeper.directQuery().allRecordsInTable())
                .containsExactlyElementsOf(sleeper.generateNumberedRecords(LongStream.range(0, 100)));
        assertThat(sleeper.tableFiles().active()).hasSize(2);
    }
}

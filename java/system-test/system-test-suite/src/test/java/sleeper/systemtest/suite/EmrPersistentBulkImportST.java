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

package sleeper.systemtest.suite;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.validation.OptionalStack;
import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.dsl.extension.AfterTestReports;
import sleeper.systemtest.dsl.reporting.SystemTestReports;
import sleeper.systemtest.suite.fixtures.SystemTestSchema;
import sleeper.systemtest.suite.testutil.Slow;
import sleeper.systemtest.suite.testutil.SystemTest;

import java.util.Map;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.BULK_IMPORT_PERSISTENT_EMR_JOB_QUEUE_URL;
import static sleeper.configuration.properties.table.TableProperty.BULK_IMPORT_MIN_LEAF_PARTITION_COUNT;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValue.addPrefix;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValue.numberStringAndZeroPadTo;
import static sleeper.systemtest.dsl.sourcedata.GenerateNumberedValueOverrides.overrideField;
import static sleeper.systemtest.suite.fixtures.SystemTestInstance.MAIN;
import static sleeper.systemtest.suite.testutil.PartitionsTestHelper.partitionsBuilder;

@SystemTest
// Slow because it needs to do two CDK deployments, one to add the EMR cluster and one to remove it.
// Each CDK deployment takes around 20 minutes.
// If we leave the EMR cluster deployed, the costs for the EMR instances add up to hundreds of pounds quite quickly.
// With the CDK deployments, the cluster doesn't stay around for very long as it only imports 100 records.
@Slow
public class EmrPersistentBulkImportST {

    @BeforeEach
    void setUp(SleeperSystemTest sleeper, AfterTestReports reporting) {
        sleeper.connectToInstance(MAIN);
        sleeper.enableOptionalStack(OptionalStack.PersistentEmrBulkImportStack);
        reporting.reportAlways(SystemTestReports.SystemTestBuilder::ingestJobs);
        // Note that we don't purge the bulk import job queue when the test fails,
        // because it is deleted when the optional stack is disabled.
    }

    @AfterEach
    void tearDown(SleeperSystemTest sleeper) {
        sleeper.disableOptionalStack(OptionalStack.PersistentEmrBulkImportStack);
    }

    @Test
    void shouldBulkImport100Records(SleeperSystemTest sleeper) {
        // Given
        sleeper.updateTableProperties(Map.of(BULK_IMPORT_MIN_LEAF_PARTITION_COUNT, "1"));
        sleeper.partitioning().setPartitions(partitionsBuilder(sleeper)
                .rootFirst("root")
                .splitToNewChildren("root", "A", "B", "row-50")
                .buildTree());
        sleeper.setGeneratorOverrides(overrideField(
                SystemTestSchema.ROW_KEY_FIELD_NAME,
                numberStringAndZeroPadTo(2).then(addPrefix("row-"))));
        sleeper.sourceFiles().createWithNumberedRecords("test.parquet", LongStream.range(0, 100));

        // When
        sleeper.ingest().bulkImportByQueue()
                .sendSourceFiles(BULK_IMPORT_PERSISTENT_EMR_JOB_QUEUE_URL, "test.parquet")
                .waitForJobs();

        // Then
        assertThat(sleeper.directQuery().allRecordsInTable())
                .containsExactlyElementsOf(sleeper.generateNumberedRecords(LongStream.range(0, 100)));
        assertThat(sleeper.tableFiles().references()).hasSize(2);
    }
}

/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.cdk.stack.bulkimport;

import org.approvaltests.Approvals;
import org.approvaltests.core.Options;
import org.junit.jupiter.api.Test;

import sleeper.cdk.stack.SleeperCoreStacks;
import sleeper.cdk.testutil.SleeperStackTestBase;

public class EksBulkImportStackIT extends SleeperStackTestBase {

    @Test
    void shouldGenerateCloudFormationTemplate() {
        // Given
        SleeperCoreStacks core = SleeperCoreStacks.create(rootStack, instanceProps());
        BulkImportBucketStack bucket = new BulkImportBucketStack(rootStack, "BulkImportBucket", instanceProperties, core);

        // When
        EksBulkImportStack stack = new EksBulkImportStack(
                rootStack, "EksBulkImport", instanceProperties, instanceArtefacts(), bucket, core);

        // Then
        Approvals.verify(printer.toJson(stack), new Options()
                .forFile().withName("eks-bulk-import", ".json"));
    }
}

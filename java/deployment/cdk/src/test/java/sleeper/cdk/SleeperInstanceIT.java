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
package sleeper.cdk;

import org.approvaltests.Approvals;
import org.approvaltests.core.Options;
import org.junit.jupiter.api.Test;

import sleeper.cdk.testutil.SleeperStackTestBase;

import static sleeper.core.properties.instance.CommonProperty.OPTIONAL_STACKS;
import static sleeper.core.properties.instance.EMRServerlessProperty.BULK_IMPORT_EMR_SERVERLESS_RELEASE;

public class SleeperInstanceIT extends SleeperStackTestBase {

    @Test
    void shouldGenerateCloudFormationTemplatesWithDefaultOptionalStacks() {
        // Given
        instanceProperties.unset(OPTIONAL_STACKS);
        instanceProperties.set(BULK_IMPORT_EMR_SERVERLESS_RELEASE, "emr-1.2.3");

        // When
        SleeperInstance.create(rootStack, instanceProps());

        // Then
        Approvals.verify(printer.toJson(rootStack), new Options()
                .withReporter((receieved, approved) -> false) // Generating diff output for failures is too slow, so skip it
                .forFile().withName("default-instance", ".json"));
    }

}

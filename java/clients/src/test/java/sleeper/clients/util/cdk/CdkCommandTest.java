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

package sleeper.clients.util.cdk;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

class CdkCommandTest {

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldPassArtefactCleanupLogPolicy(boolean retain) {
        CdkCommand command = CdkCommand.deployArtefacts("test-deployment", retain);
        assertThat(command.command()).containsExactly("deploy", "--require-approval", "never");
        assertThat(command.arguments()).containsExactly(
                "-c", "id=test-deployment", "-c", "retainLogsAfterDestroy=" + retain);
    }

    @Test
    void shouldKeepExistingArtefactsCommandUnchanged() {
        assertThat(CdkCommand.deployArtefacts("test-deployment").arguments())
                .containsExactly("-c", "id=test-deployment");
    }
}

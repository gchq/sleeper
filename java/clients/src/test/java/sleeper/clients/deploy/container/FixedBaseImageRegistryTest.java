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
package sleeper.clients.deploy.container;

import org.junit.jupiter.api.Test;

import sleeper.clients.util.command.CommandPipeline;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static sleeper.clients.testutil.RunCommandTestHelper.recordCommandsRun;

public class FixedBaseImageRegistryTest {

    private final List<CommandPipeline> commandsThatRan = new ArrayList<>();

    @Test
    void shouldUseFixedRegistry() {
        // When
        BaseImageDestination destination = fixedRegistry("fixed-registry");

        // Then
        assertThat(destination.repositoryPrefix("deployment-registry"))
                .isEqualTo("fixed-registry");
    }

    @Test
    void shouldDoNothingOnCreate() throws Exception {
        // When
        fixedRegistry("fixed-registry")
                .createIfMissing(recordCommandsRun(commandsThatRan));

        // Then
        assertThat(commandsThatRan).isEmpty();
    }

    private BaseImageDestination fixedRegistry(String baseImagePrefix) {
        return BaseImageDestination.fixedRegistry(baseImagePrefix);
    }

}

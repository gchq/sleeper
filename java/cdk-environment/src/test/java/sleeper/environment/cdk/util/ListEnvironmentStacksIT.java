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

package sleeper.environment.cdk.util;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;

@Disabled("Run manually with CDK installed and AWS credentials in environment variables")
public class ListEnvironmentStacksIT {
    @Test
    void shouldListStacks() throws Exception {
        assertThat(ListEnvironmentStacks.atDirectory(Paths.get("").toAbsolutePath())
                .stacksForEnvironment("test-environment"))
                .containsExactly("test-environment-Networking", "test-environment-BuildEC2");
    }
}

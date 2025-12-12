/*
 * Copyright 2022-2025 Crown Copyright
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

package sleeper.environment.cdk.outputs;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class StackOutputsTest {

    @Test
    void shouldConvertStackOutputsFromAndToMap() {
        Map<String, Map<String, String>> outputsByStack = Map.of(
                "stack-1", Map.of("output-1", "value-1", "output-2", "value-2"),
                "stack-2", Map.of("output-1", "value-3", "output-2", "value-4"));

        assertThat(StackOutputs.fromMap(outputsByStack).toMap())
                .isEqualTo(outputsByStack);
    }
}

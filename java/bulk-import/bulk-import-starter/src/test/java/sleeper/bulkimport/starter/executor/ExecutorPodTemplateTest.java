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

package sleeper.bulkimport.starter.executor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static org.assertj.core.api.Assertions.assertThat;

public class ExecutorPodTemplateTest {

    @Test
    void shouldSetEphemeralStorageOnExecutorContainer() throws IOException {
        // When
        String yaml = ExecutorPodTemplate.forEphemeralStorageRequestAndLimit("20Gi", "50Gi");

        // Then
        JsonNode parsed = new ObjectMapper(new YAMLFactory()).readTree(yaml);
        assertThat(yaml).doesNotContain("placeholder");
        assertThatJson(parsed)
                .inPath("$.spec.containers[0].resources.requests['ephemeral-storage']")
                .isEqualTo("20Gi");
        assertThatJson(parsed)
                .inPath("$.spec.containers[0].resources.limits['ephemeral-storage']")
                .isEqualTo("50Gi");
    }
}

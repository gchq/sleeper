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

package sleeper.build.github.containers;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

class DeleteOldGHCRContainersTest {
    @Test
    void shouldDeleteAContainer() {
        // Given
        GHCRContainer container = GHCRContainer.builder()
                .id("test-container")
                .created(Instant.parse("2023-01-20T09:30:00.001Z"))
                .tags(List.of("tag1", "tag2"))
                .build();
        // When

    }
}

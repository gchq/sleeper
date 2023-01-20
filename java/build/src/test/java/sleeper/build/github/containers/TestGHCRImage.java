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

import java.util.List;
import java.util.Map;

public class TestGHCRImage {

    private final int id;
    private final Map<String, Object> metadata;

    private TestGHCRImage(int id, List<String> tags) {
        this.id = id;
        this.metadata = Map.of("package-type", "container", "container", Map.of("tags", tags));
    }

    public static TestGHCRImage imageWithId(int id) {
        return new TestGHCRImage(id, List.of());
    }

    public static TestGHCRImage imageWithIdAndTags(int id, String... tags) {
        return new TestGHCRImage(id, List.of(tags));
    }

    public int getId() {
        return id;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }
}

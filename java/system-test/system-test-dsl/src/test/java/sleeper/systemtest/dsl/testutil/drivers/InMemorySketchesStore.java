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
package sleeper.systemtest.dsl.testutil.drivers;

import sleeper.sketches.Sketches;

import java.util.HashMap;
import java.util.Map;

public class InMemorySketchesStore {
    private final Map<String, Sketches> filenameToSketches = new HashMap<>();

    public void addSketchForFile(String filename, Sketches sketches) {
        filenameToSketches.put(filename.replace(".parquet", ".sketches"), sketches);
    }

    public Sketches load(String filename) {
        return filenameToSketches.get(filename);
    }
}

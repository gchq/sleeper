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
package sleeper.sketches.testutils;

import sleeper.core.schema.Schema;
import sleeper.sketches.Sketches;
import sleeper.sketches.store.SketchesStore;

import java.util.HashMap;
import java.util.Map;

public class InMemorySketchesStore implements SketchesStore {

    private Map<String, Sketches> filenameToSketches = new HashMap<>();

    @Override
    public void saveFileSketches(String filename, Schema schema, Sketches sketches) {
        saveFileSketches(filename, sketches);
    }

    @Override
    public Sketches loadFileSketches(String filename, Schema schema) {
        return loadFileSketches(filename);
    }

    public void saveFileSketches(String filename, Sketches sketches) {
        filenameToSketches.put(filename, sketches);
    }

    public Sketches loadFileSketches(String filename) {
        return filenameToSketches.get(filename);
    }

}

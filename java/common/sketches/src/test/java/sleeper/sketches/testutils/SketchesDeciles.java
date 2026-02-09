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

import com.google.common.base.Strings;
import org.apache.datasketches.quantiles.ItemsUnion;

import sleeper.core.properties.table.TableProperties;
import sleeper.core.row.Row;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReference;
import sleeper.sketches.Sketches;
import sleeper.sketches.store.SketchesStore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Consumer;

import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toUnmodifiableList;

public class SketchesDeciles {

    private final Map<String, SketchDeciles> decilesByField;

    private SketchesDeciles(Map<String, SketchDeciles> decilesByField) {
        this.decilesByField = decilesByField;
    }

    public static SketchesDeciles from(Sketches sketches) {
        return new SketchesDeciles(createDecilesByField(sketches));
    }

    public static SketchesDeciles from(TableProperties tableProperties, List<Row> rows) {
        return from(tableProperties.getSchema(), rows);
    }

    public static SketchesDeciles from(Schema schema, List<Row> rows) {
        Sketches sketches = Sketches.from(schema);
        for (Row row : rows) {
            sketches.update(row);
        }
        return from(sketches);
    }

    public static SketchesDeciles fromFile(Schema schema, FileReference file, SketchesStore sketchesStore) throws IOException {
        return fromFile(schema, file.getFilename(), sketchesStore);
    }

    public static SketchesDeciles fromFile(Schema schema, String file, SketchesStore sketchesStore) throws IOException {
        return from(sketchesStore.loadFileSketches(file, schema));
    }

    public static SketchesDeciles fromFileReferences(Schema schema, List<FileReference> files, SketchesStore sketchesStore) {
        return fromFiles(schema, files.stream().map(FileReference::getFilename).collect(toUnmodifiableList()), sketchesStore);
    }

    public static SketchesDeciles fromFiles(Schema schema, List<String> files, SketchesStore sketchesStore) {
        Map<String, ItemsUnion> unionByField = schema.getRowKeyFields().stream()
                .collect(toMap(Field::getName, field -> Sketches.createUnion(field.getType(), 1024)));
        for (String file : files) {
            Sketches sketches = sketchesStore.loadFileSketches(file, schema);
            sketches.fieldSketches().forEach(fieldSketch -> {
                ItemsUnion union = unionByField.get(fieldSketch.getField().getName());
                union.update(fieldSketch.getSketch());
            });
        }
        Sketches sketches = new Sketches(schema, unionByField.entrySet().stream()
                .collect(toMap(Entry::getKey, entry -> entry.getValue().getResult())));
        return from(sketches);
    }

    public static Builder builder() {
        return new Builder();
    }

    private static Map<String, SketchDeciles> createDecilesByField(Sketches sketches) {
        Map<String, SketchDeciles> decilesByField = new TreeMap<>();
        sketches.fieldSketches().forEach(fieldSketch -> {
            decilesByField.put(fieldSketch.getField().getName(), SketchDeciles.from(fieldSketch));
        });
        return decilesByField;
    }

    public SketchDeciles getDecilesByField(Field field) {
        return decilesByField.get(field.getName());
    }

    @Override
    public int hashCode() {
        return Objects.hash(decilesByField);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof SketchesDeciles)) {
            return false;
        }
        SketchesDeciles other = (SketchesDeciles) obj;
        return Objects.equals(decilesByField, other.decilesByField);
    }

    @Override
    public String toString() {
        List<String> keyDescriptions = new ArrayList<>();
        decilesByField.forEach((field, deciles) -> {
            keyDescriptions.add("Key " + field + "\n"
                    + Strings.repeat("=", field.length() + 4) + "\n"
                    + deciles + "\n");
        });
        return String.join("\n", keyDescriptions);
    }

    public static class Builder {
        private final Map<String, SketchDeciles> decilesByField = new TreeMap<>();

        private Builder() {
        }

        public Builder field(String field, Consumer<SketchDeciles.Builder> deciles) {
            SketchDeciles.Builder builder = SketchDeciles.builder();
            deciles.accept(builder);
            decilesByField.put(field, builder.build());
            return this;
        }

        public Builder fieldEmpty(String field) {
            decilesByField.put(field, SketchDeciles.empty());
            return this;
        }

        public SketchesDeciles build() {
            return new SketchesDeciles(decilesByField);
        }
    }

}

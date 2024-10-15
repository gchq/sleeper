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
package sleeper.sketches.testutils;

import com.google.common.base.Strings;
import org.apache.datasketches.quantiles.ItemsUnion;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import sleeper.core.record.Record;
import sleeper.core.schema.Field;
import sleeper.core.schema.Schema;
import sleeper.core.statestore.FileReference;
import sleeper.sketches.Sketches;
import sleeper.sketches.s3.SketchesSerDeToS3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Consumer;

import static java.util.stream.Collectors.toMap;

public class SketchesDeciles {

    private final Map<String, SketchDeciles> decilesByField;

    private SketchesDeciles(Map<String, SketchDeciles> decilesByField) {
        this.decilesByField = decilesByField;
    }

    public static SketchesDeciles from(Sketches sketches) {
        return new SketchesDeciles(createDecilesByField(sketches));
    }

    public static SketchesDeciles from(Schema schema, List<Record> records) {
        Sketches sketches = Sketches.from(schema);
        for (Record record : records) {
            sketches.update(schema, record);
        }
        return from(sketches);
    }

    public static SketchesDeciles fromFile(Schema schema, FileReference file) throws IOException {
        return fromFile(schema, file.getFilename());
    }

    public static SketchesDeciles fromFile(Schema schema, String file) throws IOException {
        return from(getSketches(schema, file));
    }

    public static SketchesDeciles fromFiles(Schema schema, List<String> files) throws IOException {
        Map<String, ItemsUnion> unionByField = schema.getRowKeyFields().stream()
                .collect(toMap(Field::getName, field -> Sketches.createUnion(field.getType(), 1024)));
        for (String file : files) {
            Sketches sketches = getSketches(schema, file);
            for (Field field : schema.getRowKeyFields()) {
                ItemsUnion union = unionByField.get(field.getName());
                union.union(sketches.getQuantilesSketch(field.getName()));
            }
        }
        Sketches sketches = new Sketches(unionByField.entrySet().stream()
                .collect(toMap(Entry::getKey, entry -> entry.getValue().getResult())));
        return from(sketches);
    }

    private static Sketches getSketches(Schema schema, String filename) throws IOException {
        String sketchFile = filename.replace(".parquet", ".sketches");
        return new SketchesSerDeToS3(schema).loadFromHadoopFS(new Path(sketchFile), new Configuration());
    }

    public static Builder builder() {
        return new Builder();
    }

    private static Map<String, SketchDeciles> createDecilesByField(Sketches sketches) {
        Map<String, SketchDeciles> decilesByField = new TreeMap<>();
        sketches.getQuantilesSketches().forEach((field, sketch) -> {
            decilesByField.put(field, SketchDeciles.from(sketch));
        });
        return decilesByField;
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

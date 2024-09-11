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

import sleeper.sketches.Sketches;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Consumer;

public class SketchesDeciles {

    private final Map<String, SketchDeciles> decilesByField;

    private SketchesDeciles(Map<String, SketchDeciles> decilesByField) {
        this.decilesByField = decilesByField;
    }

    public static SketchesDeciles from(Sketches sketches) {
        return new SketchesDeciles(createDecilesByField(sketches));
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

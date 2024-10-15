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

import com.facebook.collections.ByteArray;
import org.apache.datasketches.quantiles.ItemsSketch;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

public class SketchDeciles {

    private static final double[] DECILES_QUANTILE_BOUNDARIES = new double[]{
        0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0
    };

    private final Object min;
    private final Object max;
    private final Map<Double, Object> decileByRank;

    private SketchDeciles(Object min, Object max, Map<Double, Object> decileByRank) {
        this.min = min;
        this.max = max;
        this.decileByRank = decileByRank;
    }

    public static SketchDeciles from(ItemsSketch<?> sketch) {
        if (sketch.isEmpty()) {
            return empty();
        }
        return new SketchDeciles(sketch.getMinItem(), sketch.getMaxItem(), readDecilesByRank(sketch));
    }

    public static Builder builder() {
        return new Builder();
    }

    public static SketchDeciles empty() {
        return builder().build();
    }

    private static Map<Double, Object> readDecilesByRank(ItemsSketch<?> sketch) {
        Object[] values = sketch.getQuantiles(DECILES_QUANTILE_BOUNDARIES);
        if (values == null) {
            return Map.of();
        }
        Map<Double, Object> decilesByRank = new LinkedHashMap<>();
        for (int i = 0; i <= 10; i++) {
            decilesByRank.put(DECILES_QUANTILE_BOUNDARIES[i], values[i]);
        }
        return decilesByRank;
    }

    @Override
    public int hashCode() {
        return Objects.hash(min, max, decileByRank);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof SketchDeciles)) {
            return false;
        }
        SketchDeciles other = (SketchDeciles) obj;
        return Objects.equals(min, other.min) && Objects.equals(max, other.max) && Objects.equals(decileByRank, other.decileByRank);
    }

    @Override
    public String toString() {
        List<String> lines = new ArrayList<>(decileByRank.size() + 2);
        lines.add("Min: " + min);
        lines.add("Max: " + max);
        decileByRank.forEach((rank, value) -> {
            lines.add("Decile " + rank + ": " + value);
        });
        return String.join("\n", lines);
    }

    public static class Builder {
        private Object min;
        private Object max;
        private final Map<Double, Object> decileByRank = new TreeMap<>();

        private Builder() {
        }

        public Builder min(Object min) {
            this.min = min;
            decileByRank.put(0.0, min);
            return this;
        }

        public Builder max(Object max) {
            this.max = max;
            decileByRank.put(1.0, max);
            return this;
        }

        public Builder rank(double rank, Object value) {
            decileByRank.put(rank, value);
            return this;
        }

        public Builder minBytes(int... values) {
            return min(bytes(values));
        }

        public Builder maxBytes(int... values) {
            return max(bytes(values));
        }

        public Builder rankBytes(double rank, int... values) {
            return rank(rank, bytes(values));
        }

        public SketchDeciles build() {
            return new SketchDeciles(min, max, decileByRank);
        }
    }

    private static ByteArray bytes(int... values) {
        byte[] bytes = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            bytes[i] = (byte) values[i];
        }
        return ByteArray.wrap(bytes);
    }
}

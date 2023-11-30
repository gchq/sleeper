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
package sleeper.sketches.testutils;

import org.apache.datasketches.quantiles.ItemsSketch;

import sleeper.sketches.Sketches;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

public class AssertQuantiles {
    private final ItemsSketch<?> sketch;
    private final Object minValue;
    private final Object maxValue;
    private final List<ExpectedQuantile> quantiles;

    private AssertQuantiles(Builder builder) {
        sketch = builder.sketch;
        minValue = builder.minValue;
        maxValue = builder.maxValue;
        quantiles = builder.quantiles;
    }

    public static Builder forSketch(ItemsSketch<?> sketch) {
        return new Builder(sketch);
    }

    private static final double[] DECILES_QUANTILE_BOUNDARIES = new double[]{
            0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0
    };

    public static Map<String, Map<Double, Object>> asDecilesMaps(Sketches sketches) {
        return sketches.getQuantilesSketches().entrySet().stream()
                .map(entry -> Map.entry(entry.getKey(), asDecilesMap(entry.getValue())))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public static Map<Double, Object> asDecilesMap(ItemsSketch<?> sketch) {
        Object[] values = sketch.getQuantiles(DECILES_QUANTILE_BOUNDARIES);
        return IntStream.rangeClosed(0, 10)
                .mapToObj(i -> Map.entry(DECILES_QUANTILE_BOUNDARIES[i], values[i]))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private void verify() {
        assertThat(sketch)
                .extracting(ItemsSketch::getMinValue, ItemsSketch::getMaxValue)
                .describedAs("min, max")
                .containsExactly(minValue, maxValue);
        double[] ranks = ranksArray();
        assertThat(sketch.getQuantiles(ranks))
                .describedAs("quantiles with ranks %s", Arrays.toString(ranks))
                .isEqualTo(valuesArray());
    }

    private double[] ranksArray() {
        return quantiles.stream()
                .mapToDouble(ExpectedQuantile::getRank)
                .toArray();
    }

    private Object[] valuesArray() {
        return quantiles.stream()
                .map(ExpectedQuantile::getValue)
                .toArray();
    }

    public static final class Builder {
        private final ItemsSketch<?> sketch;
        private Object minValue;
        private Object maxValue;
        private final List<ExpectedQuantile> quantiles = new ArrayList<>();

        private Builder(ItemsSketch<?> sketch) {
            this.sketch = sketch;
        }

        public Builder min(Object minValue) {
            this.minValue = minValue;
            return this;
        }

        public Builder max(Object maxValue) {
            this.maxValue = maxValue;
            return this;
        }

        public Builder quantile(double rank, Object value) {
            quantiles.add(new ExpectedQuantile(rank, value));
            return this;
        }

        public void verify() {
            build().verify();
        }

        private AssertQuantiles build() {
            return new AssertQuantiles(this);
        }
    }
}

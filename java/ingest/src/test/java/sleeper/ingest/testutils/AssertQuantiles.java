/*
 * Copyright 2022 Crown Copyright
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
package sleeper.ingest.testutils;

import org.apache.datasketches.quantiles.ItemsSketch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class AssertQuantiles {
    private static final double[] QUANTILE_RANGE = new double[]{0.0D, 0.1D, 0.2D, 0.3D, 0.4D, 0.5D, 0.6D, 0.7D, 0.8D, 0.9D};
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

        public Builder quantile(double quantile, Object value) {
            quantiles.add(new ExpectedQuantile(quantile, value));
            return this;
        }

        public Builder quantiles(List<Object> values) {
            int index = 0;
            for (double q : QUANTILE_RANGE) {
                this.quantile(q, values.get(index++));
            }
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

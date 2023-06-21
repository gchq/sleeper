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
package sleeper.bulkimport.configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class EmrInstanceTypeConfig {

    private final String instanceType;
    private final Integer weightedCapacity;

    private EmrInstanceTypeConfig(Builder builder) {
        instanceType = builder.instanceType;
        weightedCapacity = builder.weightedCapacity;
    }

    public static Stream<EmrInstanceTypeConfig> readInstanceTypesProperty(List<String> instanceTypeEntries) {
        Builder builder = null;
        List<Builder> builders = new ArrayList<>();
        for (String entry : instanceTypeEntries) {
            try {
                int capacity = Integer.parseInt(entry);
                if (builder == null) {
                    throw new IllegalArgumentException("Instance type capacity given without an instance type: " + entry);
                }
                builder.weightedCapacity(capacity);
            } catch (NumberFormatException e) {
                builder = builder().instanceType(entry);
                builders.add(builder);
            }
        }
        return builders.stream().map(Builder::build);
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getInstanceType() {
        return instanceType;
    }

    public Integer getWeightedCapacity() {
        return weightedCapacity;
    }

    public static final class Builder {
        private String instanceType;
        private Integer weightedCapacity;

        private Builder() {
        }

        public Builder instanceType(String instanceType) {
            this.instanceType = instanceType;
            return this;
        }

        public Builder weightedCapacity(Integer weightedCapacity) {
            this.weightedCapacity = weightedCapacity;
            return this;
        }

        public EmrInstanceTypeConfig build() {
            return new EmrInstanceTypeConfig(this);
        }
    }
}

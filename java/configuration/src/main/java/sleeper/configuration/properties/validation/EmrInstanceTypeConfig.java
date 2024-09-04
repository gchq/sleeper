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
package sleeper.configuration.properties.validation;

import sleeper.configuration.properties.SleeperProperties;
import sleeper.configuration.properties.SleeperProperty;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static sleeper.configuration.properties.SleeperPropertyValues.readList;
import static sleeper.configuration.properties.validation.EmrInstanceArchitecture.ARM64;
import static sleeper.configuration.properties.validation.EmrInstanceArchitecture.X86_64;

public class EmrInstanceTypeConfig {
    private final EmrInstanceArchitecture architecture;
    private final String instanceType;
    private final Integer weightedCapacity;

    private EmrInstanceTypeConfig(Builder builder) {
        architecture = Objects.requireNonNull(builder.architecture, "architecture must not be null");
        instanceType = Objects.requireNonNull(builder.instanceType, "instanceType must not be null");
        weightedCapacity = builder.weightedCapacity;
    }

    public static <T extends SleeperProperty> Stream<EmrInstanceTypeConfig> readInstanceTypes(
            SleeperProperties<T> properties, T architectureProperty, T x86Property, T armProperty) {
        return properties.streamEnumList(architectureProperty, EmrInstanceArchitecture.class)
                .flatMap(architecture -> {
                    if (architecture == ARM64) {
                        return readInstanceTypesProperty(properties.getList(armProperty), architecture);
                    } else {
                        return readInstanceTypesProperty(properties.getList(x86Property), architecture);
                    }
                });
    }

    public static Stream<EmrInstanceTypeConfig> readInstanceTypesProperty(List<String> instanceTypeEntries, EmrInstanceArchitecture architecture) {
        Builder builder = null;
        List<Builder> builders = new ArrayList<>();
        for (String entry : instanceTypeEntries) {
            try {
                int capacity = Integer.parseInt(entry);
                if (builder == null) {
                    throw new IllegalArgumentException("Instance type capacity given without an instance type: " + entry);
                }
                builder.weightedCapacity(capacity).architecture(architecture);
            } catch (NumberFormatException e) {
                builder = builder().instanceType(entry).architecture(architecture);
                builders.add(builder);
            }
        }
        return builders.stream().map(Builder::build);
    }

    public static boolean isValidInstanceTypes(String value) {
        if (value == null) {
            return false;
        }
        try {
            List<String> instanceTypes = readInstanceTypesProperty(readList(value), X86_64)
                    .map(EmrInstanceTypeConfig::getInstanceType)
                    .collect(Collectors.toUnmodifiableList());
            return instanceTypes.size() == instanceTypes.stream().distinct().count();
        } catch (IllegalArgumentException e) {
            return false;
        }
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

    public EmrInstanceArchitecture getArchitecture() {
        return architecture;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        EmrInstanceTypeConfig that = (EmrInstanceTypeConfig) o;

        if (!instanceType.equals(that.instanceType)) {
            return false;
        }
        return Objects.equals(weightedCapacity, that.weightedCapacity);
    }

    @Override
    public int hashCode() {
        int result = instanceType.hashCode();
        result = 31 * result + (weightedCapacity != null ? weightedCapacity.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "EmrInstanceTypeConfig{" +
                "instanceType='" + instanceType + '\'' +
                ", weightedCapacity=" + weightedCapacity +
                '}';
    }

    public static final class Builder {
        private String instanceType;
        private Integer weightedCapacity;
        private EmrInstanceArchitecture architecture;

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

        public Builder architecture(EmrInstanceArchitecture architecture) {
            this.architecture = architecture;
            return this;
        }

        public EmrInstanceTypeConfig build() {
            return new EmrInstanceTypeConfig(this);
        }
    }
}

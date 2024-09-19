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

package sleeper.configuration.properties.deploy;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;

import java.util.List;
import java.util.Objects;

public class DeployInstanceConfiguration {
    private final InstanceProperties instanceProperties;
    private final List<TableProperties> tableProperties;

    private DeployInstanceConfiguration(Builder builder) {
        instanceProperties = builder.instanceProperties;
        tableProperties = builder.tableProperties;
    }

    public DeployInstanceConfiguration(InstanceProperties instanceProperties, TableProperties tableProperties) {
        this(builder().instanceProperties(instanceProperties).tableProperties(tableProperties));
    }

    public static Builder builder() {
        return new Builder();
    }

    public InstanceProperties getInstanceProperties() {
        return instanceProperties;
    }

    public List<TableProperties> getTableProperties() {
        return tableProperties;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DeployInstanceConfiguration that = (DeployInstanceConfiguration) o;
        return Objects.equals(instanceProperties, that.instanceProperties) && Objects.equals(tableProperties, that.tableProperties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(instanceProperties, tableProperties);
    }

    @Override
    public String toString() {
        return "DeployInstanceConfiguration{" +
                "instanceProperties=" + instanceProperties +
                ", tableProperties=" + tableProperties +
                '}';
    }

    public static final class Builder {
        private InstanceProperties instanceProperties;
        private List<TableProperties> tableProperties;

        public Builder() {
        }

        public Builder instanceProperties(InstanceProperties instanceProperties) {
            this.instanceProperties = instanceProperties;
            return this;
        }

        public Builder tableProperties(List<TableProperties> tableProperties) {
            this.tableProperties = tableProperties;
            return this;
        }

        public Builder tableProperties(TableProperties tableProperties) {
            this.tableProperties = List.of(tableProperties);
            return this;
        }

        public DeployInstanceConfiguration build() {
            return new DeployInstanceConfiguration(this);
        }
    }
}

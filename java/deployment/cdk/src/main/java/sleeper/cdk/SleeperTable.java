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
package sleeper.cdk;

import sleeper.core.schema.Schema;

public class SleeperTable {

    private String tableName;
    private String instanceId;
    private Schema schema;

    public SleeperTable(Builder builder) {
        this.tableName = builder.tableName;
        this.instanceId = builder.instanceId;
        this.schema = builder.schema;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        String tableName;
        String instanceId;
        Schema schema;

        private Builder() {

        }

        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder instanceId(String instanceId) {
            this.instanceId = instanceId;
            return this;
        }

        public Builder schema(Schema schema) {
            this.schema = schema;
            return this;
        }

        public SleeperTable build() {
            return new SleeperTable(this);
        }

    }
}

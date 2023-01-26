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

package sleeper.arrow.record;

import org.apache.arrow.vector.VectorSchemaRoot;

import sleeper.arrow.schema.SchemaWrapper;
import sleeper.core.record.Record;

import java.util.ArrayList;
import java.util.List;

import static sleeper.arrow.record.RecordConverter.getValueFromFieldVector;

public class RecordBackedByArrow {
    private SchemaWrapper schemaWrapper;
    private VectorSchemaRoot vectorSchemaRoot;
    private int rowNum;
    private List<Record> recordList = new ArrayList<>();

    private RecordBackedByArrow(Builder builder) {
        schemaWrapper = builder.schemaWrapper;
        vectorSchemaRoot = builder.vectorSchemaRoot;
    }

    public static Builder builder() {
        return new Builder();
    }

    public Object get(String fieldName) {
        return getValueFromFieldVector(vectorSchemaRoot.getVector(fieldName), rowNum);
    }

    public static final class Builder {
        private SchemaWrapper schemaWrapper;
        private VectorSchemaRoot vectorSchemaRoot;
        private int rowNum;

        private Builder() {
        }

        public Builder schemaWrapper(SchemaWrapper schemaWrapper) {
            this.schemaWrapper = schemaWrapper;
            return this;
        }

        public Builder vectorSchemaRoot(VectorSchemaRoot vectorSchemaRoot) {
            this.vectorSchemaRoot = vectorSchemaRoot;
            return this;
        }

        public Builder rowNum(int rowNum) {
            this.rowNum = rowNum;
            return this;
        }

        public RecordBackedByArrow build() {
            return new RecordBackedByArrow(this);
        }
    }
}

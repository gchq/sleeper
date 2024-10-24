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
package sleeper.bulkexport.model;

import java.util.List;

import javax.swing.plaf.synth.Region;

/**
 * Model.
 */
public class BulkExportQuery {
    private final String tableName;
    private final String exportId;
    private final List<Region> regions;

    private BulkExportQuery(Builder builder) {
        tableName = builder.tableName;
        exportId = builder.exportId;
        regions = builder.regions;

    }

    @Override
    public String toString() {
        return "BulkExportQuery{}";
    }

    /**
     * Builder.
     */
    public static final class Builder {
        private String tableName;
        private String exportId;
        private List<Region> regions;

        private Builder() {
        }

        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder exportId(String exportId) {
            this.exportId = exportId;
            return this;
        }

        public Builder regions(List<Region> regions) {
            this.regions = regions;
            return this;
        }

        public BulkExportQuery build() {
            return new BulkExportQuery(this);
        }
    }

}

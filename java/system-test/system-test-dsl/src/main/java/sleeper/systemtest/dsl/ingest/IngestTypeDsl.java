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

package sleeper.systemtest.dsl.ingest;

import sleeper.systemtest.dsl.instance.SystemTestInstanceContext;

import java.util.Map;

import static sleeper.core.properties.table.TableProperty.INGEST_PARTITION_FILE_WRITER_TYPE;
import static sleeper.core.properties.table.TableProperty.INGEST_ROW_BATCH_TYPE;

public class IngestTypeDsl {

    private final String fileWriterType;
    private final String rowBatchType;

    private IngestTypeDsl(String fileWriterType, String rowBatchType) {
        this.fileWriterType = fileWriterType;
        this.rowBatchType = rowBatchType;
    }

    public void applyTo(SystemTestInstanceContext instance) {
        instance.updateTableProperties(Map.of(
                INGEST_ROW_BATCH_TYPE, rowBatchType,
                INGEST_PARTITION_FILE_WRITER_TYPE, fileWriterType));
    }

    public static IngestTypeDsl directWriteBackedByArrow() {
        return new IngestTypeDsl("direct", "arrow");
    }

    public static IngestTypeDsl asyncWriteBackedByArrow() {
        return new IngestTypeDsl("async", "arrow");
    }

    public static IngestTypeDsl directWriteBackedByArrayList() {
        return new IngestTypeDsl("direct", "arraylist");
    }
}

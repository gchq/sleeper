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

package sleeper.io.parquet.arrow;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

import sleeper.arrow.record.RecordBackedByArrow;
import sleeper.arrow.schema.SchemaBackedByArrow;

import java.util.HashMap;

public class ArrowWriteSupport extends WriteSupport<RecordBackedByArrow> {
    private final MessageType messageType;
    private final SchemaBackedByArrow schemaBackedByArrow;
    private ArrowWriter arrowWriter;

    public ArrowWriteSupport(MessageType messageType, SchemaBackedByArrow schemaBackedByArrow) {
        this.messageType = messageType;
        this.schemaBackedByArrow = schemaBackedByArrow;
    }

    public WriteContext init(Configuration configuration) {
        return new WriteContext(messageType, new HashMap<>());
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
        arrowWriter = new ArrowWriter(recordConsumer, schemaBackedByArrow);
    }

    @SuppressFBWarnings({"NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE", "UWF_FIELD_NOT_INITIALIZED_IN_CONSTRUCTOR"})
    public void write(RecordBackedByArrow record) {
        arrowWriter.write(record);
    }
}

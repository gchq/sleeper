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
package sleeper.core.row.serialiser;

import org.apache.commons.codec.binary.Base64;

import sleeper.core.row.ResultsBatch;
import sleeper.core.row.Row;
import sleeper.core.schema.Schema;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Serialises and deserialises a list of records to and from a Base64 encoded string.
 */
public class Base64ResultsBatchSerialiser implements ResultsBatchSerialiser {
    private final RowSerialiser rowSerialiser;
    private final Schema schema;

    public Base64ResultsBatchSerialiser(Schema schema) {
        this.schema = schema;
        this.rowSerialiser = new RowSerialiser(schema);
    }

    @Override
    public String serialise(ResultsBatch resultsBatch) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);
            dos.writeUTF(resultsBatch.getQueryId());
            int numRecords = resultsBatch.getRows().size();
            dos.writeInt(numRecords);
            for (Row row : resultsBatch.getRows()) {
                byte[] serialisedValue = rowSerialiser.serialise(row);
                dos.writeInt(serialisedValue.length);
                dos.write(serialisedValue);
            }
            dos.close();
            byte[] bytes = baos.toByteArray();
            return Base64.encodeBase64String(bytes);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public ResultsBatch deserialise(String serialisedRecords) {
        try {
            byte[] bytes = Base64.decodeBase64(serialisedRecords);
            ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
            DataInputStream dis = new DataInputStream(bais);
            String queryId = dis.readUTF();
            int numRecords = dis.readInt();
            List<Row> rows = new ArrayList<>(numRecords);
            for (int i = 0; i < numRecords; i++) {
                int length = dis.readInt();
                byte[] serialisedRow = new byte[length];
                dis.readFully(serialisedRow);
                rows.add(rowSerialiser.deserialise(serialisedRow));
            }
            dis.close();
            return new ResultsBatch(queryId, schema, rows);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}

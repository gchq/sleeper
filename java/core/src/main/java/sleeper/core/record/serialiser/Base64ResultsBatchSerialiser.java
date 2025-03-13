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
package sleeper.core.record.serialiser;

import org.apache.commons.codec.binary.Base64;

import sleeper.core.record.Record;
import sleeper.core.record.ResultsBatch;
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
    private final RecordSerialiser recordSerialiser;
    private final Schema schema;

    public Base64ResultsBatchSerialiser(Schema schema) {
        this.schema = schema;
        this.recordSerialiser = new RecordSerialiser(schema);
    }

    @Override
    public String serialise(ResultsBatch resultsBatch) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);
            dos.writeUTF(resultsBatch.getQueryId());
            int numRecords = resultsBatch.getRecords().size();
            dos.writeInt(numRecords);
            for (Record record : resultsBatch.getRecords()) {
                byte[] serialisedValue = recordSerialiser.serialise(record);
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
            List<Record> records = new ArrayList<>(numRecords);
            for (int i = 0; i < numRecords; i++) {
                int length = dis.readInt();
                byte[] serialisedRecord = new byte[length];
                dis.readFully(serialisedRecord);
                records.add(recordSerialiser.deserialise(serialisedRecord));
            }
            dis.close();
            return new ResultsBatch(queryId, schema, records);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}

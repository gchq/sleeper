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
package sleeper.core.statestore;

import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.core.schema.type.StringType;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Serialises a {@link FileInfo} to and from a <code>byte[]</code>.
 */
public class FileInfoSerDe {

    public byte[] serialiseFileInfo(FileInfo fileInfo) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        List<PrimitiveType> rowKeyTypes = fileInfo.getRowKeyTypes();
        dos.writeInt(rowKeyTypes.size());
        int count = 0;
        for (PrimitiveType type : rowKeyTypes) {
            dos.writeUTF(type.getClass().getSimpleName());
        }
        dos.writeUTF(fileInfo.getFilename());
        dos.writeUTF(fileInfo.getFileStatus().toString());
        dos.writeLong(fileInfo.getNumberOfRecords());
        dos.writeUTF(fileInfo.getPartitionId());
        if (null != fileInfo.getJobId()) {
            dos.writeBoolean(true);
            dos.writeUTF(fileInfo.getJobId());
        } else {
            dos.writeBoolean(false);
        }
        dos.close();
        return baos.toByteArray();
    }

    public FileInfo deserialiseFileInfo(byte[] bytes) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        DataInputStream dis = new DataInputStream(bais);
        int numRowKeys = dis.readInt();
        List<PrimitiveType> rowKeyTypes = new ArrayList<>();
        for (int i = 0; i < numRowKeys; i++) {
            String simpleClassName = dis.readUTF();
            PrimitiveType keyType;
            if (simpleClassName.equals(IntType.class.getSimpleName())) {
                keyType = new IntType();
            } else if (simpleClassName.equals(LongType.class.getSimpleName())) {
                keyType = new LongType();
            } else if (simpleClassName.equals(StringType.class.getSimpleName())) {
                keyType = new StringType();
            } else if (simpleClassName.equals(ByteArrayType.class.getSimpleName())) {
                keyType = new ByteArrayType();
            } else {
                throw new IOException("Unknown type of " + simpleClassName);
            }
            rowKeyTypes.add(keyType);
        }

        return FileInfo.builder()
                .rowKeyTypes(rowKeyTypes)
                .filename(dis.readUTF())
                .fileStatus(FileInfo.FileStatus.valueOf(dis.readUTF()))
                .numberOfRecords(dis.readLong())
                .partitionId(dis.readUTF())
                .jobId(dis.readBoolean() ? dis.readUTF() : null)
                .build();
    }
}

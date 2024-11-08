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
package sleeper.compaction.core.job;

import org.apache.commons.codec.binary.Base64;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Serialises and deserialises a compaction job to and from a JSON string.
 */
public class CompactionJobSerDe {

    private CompactionJobSerDe() {
    }

    public static String serialiseToString(CompactionJob compactionJob) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeUTF(compactionJob.getTableId());
        dos.writeUTF(compactionJob.getId());
        dos.writeUTF(compactionJob.getPartitionId());
        dos.writeInt(compactionJob.getInputFiles().size());
        for (String inputFile : compactionJob.getInputFiles()) {
            dos.writeUTF(inputFile);
        }
        if (null == compactionJob.getIteratorClassName()) {
            dos.writeBoolean(true);
        } else {
            dos.writeBoolean(false);
            dos.writeUTF(compactionJob.getIteratorClassName());
        }
        if (null == compactionJob.getIteratorConfig()) {
            dos.writeBoolean(true);
        } else {
            dos.writeBoolean(false);
            dos.writeUTF(compactionJob.getIteratorConfig());
        }
        dos.writeUTF(compactionJob.getOutputFile());
        dos.close();

        return Base64.encodeBase64String(baos.toByteArray());
    }

    public static CompactionJob deserialiseFromString(String serialisedJob) throws IOException {
        byte[] bytes = Base64.decodeBase64(serialisedJob);
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        DataInputStream dis = new DataInputStream(bais);
        String tableId = dis.readUTF();
        CompactionJob.Builder compactionJobBuilder = CompactionJob.builder()
                .tableId(tableId)
                .jobId(dis.readUTF())
                .partitionId(dis.readUTF());
        int numInputFiles = dis.readInt();
        List<String> inputFiles = new ArrayList<>(numInputFiles);
        for (int i = 0; i < numInputFiles; i++) {
            inputFiles.add(dis.readUTF());
        }
        compactionJobBuilder.inputFiles(inputFiles)
                .iteratorClassName(!dis.readBoolean() ? dis.readUTF() : null)
                .iteratorConfig(!dis.readBoolean() ? dis.readUTF() : null);
        compactionJobBuilder.outputFile(dis.readUTF());
        dis.close();
        return compactionJobBuilder.build();
    }
}

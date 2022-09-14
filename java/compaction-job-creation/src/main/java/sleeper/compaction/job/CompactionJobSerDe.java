/*
 * Copyright 2022 Crown Copyright
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
package sleeper.compaction.job;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.tuple.MutablePair;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.schema.Schema;
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
 * Serialised a {@link CompactionJob} to and from a JSON {@link String}.
 */
public class CompactionJobSerDe {
    private final TablePropertiesProvider tablePropertiesProvider;

    public CompactionJobSerDe(TablePropertiesProvider tablePropertiesProvider) {
        this.tablePropertiesProvider = tablePropertiesProvider;
    }

    public String serialiseToString(CompactionJob compactionJob) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeUTF(compactionJob.getTableName());
        dos.writeUTF(compactionJob.getId());
        dos.writeUTF(compactionJob.getPartitionId());
        dos.writeInt(compactionJob.getInputFiles().size());
        for (String inputFile : compactionJob.getInputFiles()) {
            dos.writeUTF(inputFile);
        }
        dos.writeBoolean(compactionJob.isSplittingJob());
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
        if (compactionJob.isSplittingJob()) {
            dos.writeInt(compactionJob.getDimension());
            Schema schema = tablePropertiesProvider.getTableProperties(compactionJob.getTableName()).getSchema();
            PrimitiveType type = (PrimitiveType) schema.getRowKeyFields().get(compactionJob.getDimension()).getType();
            if (type instanceof IntType) {
                dos.writeInt((int) compactionJob.getSplitPoint());
            } else if (type instanceof LongType) {
                dos.writeLong((long) compactionJob.getSplitPoint());
            } else if (type instanceof StringType) {
                dos.writeUTF((String) compactionJob.getSplitPoint());
            } else if (type instanceof ByteArrayType) {
                byte[] splitPoint = (byte[]) compactionJob.getSplitPoint();
                dos.writeInt(splitPoint.length);
                dos.write(splitPoint);
            } else {
                throw new IllegalArgumentException("Unknown type " + type);
            }
            dos.writeInt(compactionJob.getChildPartitions().size());
            for (String childPartition : compactionJob.getChildPartitions()) {
                dos.writeUTF(childPartition);
            }
            dos.writeUTF(compactionJob.getOutputFiles().getLeft());
            dos.writeUTF(compactionJob.getOutputFiles().getRight());
        } else {
            dos.writeUTF(compactionJob.getOutputFile());
        }
        dos.close();

        return Base64.encodeBase64String(baos.toByteArray());
    }

    public CompactionJob deserialiseFromString(String serialisedJob) throws IOException {
        byte[] bytes = Base64.decodeBase64(serialisedJob);
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        DataInputStream dis = new DataInputStream(bais);
        CompactionJob compactionJob = new CompactionJob(dis.readUTF(), dis.readUTF());
        compactionJob.setPartitionId(dis.readUTF());
        int numInputFiles = dis.readInt();
        List<String> inputFiles = new ArrayList<>(numInputFiles);
        for (int i = 0; i < numInputFiles; i++) {
            inputFiles.add(dis.readUTF());
        }
        compactionJob.setInputFiles(inputFiles);
        compactionJob.setIsSplittingJob(dis.readBoolean());
        if (!dis.readBoolean()) {
            compactionJob.setIteratorClassName(dis.readUTF());
        }
        if (!dis.readBoolean()) {
            compactionJob.setIteratorConfig(dis.readUTF());
        }
        if (compactionJob.isSplittingJob()) {
            compactionJob.setDimension(dis.readInt());
            Schema schema = tablePropertiesProvider.getTableProperties(compactionJob.getTableName()).getSchema();
            PrimitiveType type = (PrimitiveType) schema.getRowKeyFields().get(compactionJob.getDimension()).getType();
            if (type instanceof IntType) {
                compactionJob.setSplitPoint(dis.readInt());
            } else if (type instanceof LongType) {
                compactionJob.setSplitPoint(dis.readLong());
            } else if (type instanceof StringType) {
                compactionJob.setSplitPoint(dis.readUTF());
            } else if (type instanceof ByteArrayType) {
                byte[] splitPoint = new byte[dis.readInt()];
                dis.readFully(splitPoint);
                compactionJob.setSplitPoint(splitPoint);
            } else {
                throw new IllegalArgumentException("Unknown type " + type);
            }
            int numChildPartitions = dis.readInt();
            List<String> childPartitions = new ArrayList<>(numChildPartitions);
            for (int i = 0; i < numChildPartitions; i++) {
                childPartitions.add(dis.readUTF());
            }
            compactionJob.setChildPartitions(childPartitions);
            MutablePair<String, String> outputFiles = new MutablePair<>();
            outputFiles.setLeft(dis.readUTF());
            outputFiles.setRight(dis.readUTF());
            compactionJob.setOutputFiles(outputFiles);
        } else {
            compactionJob.setOutputFile(dis.readUTF());
        }
        dis.close();

        return compactionJob;
    }
}

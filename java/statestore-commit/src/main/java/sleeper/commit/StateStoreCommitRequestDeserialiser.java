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
package sleeper.commit;

import com.google.gson.Gson;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.compaction.job.CompactionJob;
import sleeper.compaction.job.CompactionJobJsonSerDe;
import sleeper.compaction.job.commit.CompactionJobCommitRequest;
import sleeper.compaction.job.commit.CompactionJobIdAssignmentCommitRequest;
import sleeper.configuration.properties.table.TablePropertiesProvider;
import sleeper.core.partition.Partition;
import sleeper.core.partition.PartitionSerDe.PartitionJsonSerDe;
import sleeper.core.statestore.commit.CommitRequestType;
import sleeper.core.statestore.commit.SplitPartitionCommitRequest;
import sleeper.core.statestore.commit.StateStoreCommitRequestInS3;
import sleeper.core.util.GsonConfig;
import sleeper.ingest.job.commit.IngestAddFilesCommitRequest;

import java.lang.reflect.Type;

/**
 * Deserialises a state store commit request.
 */
public class StateStoreCommitRequestDeserialiser {
    public static final Logger LOGGER = LoggerFactory.getLogger(StateStoreCommitRequestDeserialiser.class);

    private final Gson gson;

    public StateStoreCommitRequestDeserialiser(TablePropertiesProvider tablePropertiesProvider) {
        gson = GsonConfig.standardBuilder()
                .registerTypeAdapter(CompactionJob.class, new CompactionJobJsonSerDe())
                .registerTypeAdapter(StateStoreCommitRequest.class, new WrapperDeserialiser())
                .registerTypeAdapter(SplitPartitionCommitRequest.class, new SplitPartitionDeserialiser(tablePropertiesProvider))
                .serializeNulls()
                .create();
    }

    /**
     * Deserialises a state store commit request.
     *
     * @param  jsonString the JSON string
     * @return            a commit request
     */
    public StateStoreCommitRequest fromJson(String jsonString) {
        return gson.fromJson(jsonString, StateStoreCommitRequest.class);
    }

    /**
     * Deserialises the commit request by reading the type.
     */
    private static class WrapperDeserialiser implements JsonDeserializer<StateStoreCommitRequest> {

        @Override
        public StateStoreCommitRequest deserialize(JsonElement json, Type wrapperType, JsonDeserializationContext context) throws JsonParseException {

            JsonObject object = json.getAsJsonObject();
            CommitRequestType type = context.deserialize(object.get("type"), CommitRequestType.class);
            JsonObject requestObj = object.getAsJsonObject("request");
            if (type == null) {
                LOGGER.warn("Attempted to read an unrecognised type, JSON: {}", json);
                throw new CommitRequestValidationException("Unrecognised request type");
            }
            switch (type) {
                case COMPACTION_FINISHED:
                    return StateStoreCommitRequest.forCompactionJob(
                            context.deserialize(requestObj, CompactionJobCommitRequest.class));
                case INGEST_ADD_FILES:
                    return StateStoreCommitRequest.forIngestAddFiles(
                            context.deserialize(requestObj, IngestAddFilesCommitRequest.class));
                case STORED_IN_S3:
                    return StateStoreCommitRequest.storedInS3(
                            context.deserialize(requestObj, StateStoreCommitRequestInS3.class));
                case COMPACTION_JOB_ID_ASSIGNMENT:
                    return StateStoreCommitRequest.forCompactionJobIdAssignment(
                            context.deserialize(requestObj, CompactionJobIdAssignmentCommitRequest.class));
                case SPLIT_PARTITION:
                    return StateStoreCommitRequest.forSplitPartition(
                            context.deserialize(requestObj, SplitPartitionCommitRequest.class));
                default:
                    throw new CommitRequestValidationException("Unrecognised request type");
            }
        }
    }

    /**
     * Deserialise the split partition request.
     */
    private static class SplitPartitionDeserialiser implements JsonDeserializer<SplitPartitionCommitRequest> {

        public static final String TABLE_ID = "tableId";
        public static final String PARENT_PARTITION = "parentPartition";
        public static final String LEFT_PARTITION = "leftChild";
        public static final String RIGHT_PARTITION = "rightChild";

        private TablePropertiesProvider tablePropertiesProvider;

        private SplitPartitionDeserialiser(TablePropertiesProvider tablePropertiesProvider) {
            this.tablePropertiesProvider = tablePropertiesProvider;
        }

        @Override
        public SplitPartitionCommitRequest deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext context) throws JsonParseException {
            JsonObject json = jsonElement.getAsJsonObject();
            String tableId = json.get(TABLE_ID).getAsString();

            PartitionJsonSerDe partitionJsonSerDe = new PartitionJsonSerDe(tablePropertiesProvider.getById(tableId).getSchema());

            JsonElement jsonParentPartition = json.get(PARENT_PARTITION);
            Partition parentPartition = partitionJsonSerDe.deserialize(jsonParentPartition, type, context);

            JsonElement jsonLeftPartition = json.get(LEFT_PARTITION);
            Partition leftChildPartition = partitionJsonSerDe.deserialize(jsonLeftPartition, type, context);

            JsonElement jsonRightPartition = json.get(RIGHT_PARTITION);
            Partition rightChildPartition = partitionJsonSerDe.deserialize(jsonRightPartition, type, context);

            return new SplitPartitionCommitRequest(tableId, parentPartition, leftChildPartition, rightChildPartition);
        }
    }
}

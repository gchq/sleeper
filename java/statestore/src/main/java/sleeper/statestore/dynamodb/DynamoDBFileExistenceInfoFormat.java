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
package sleeper.statestore.dynamodb;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import sleeper.statestore.FileLifecycleInfo;
import sleeper.statestore.StateStoreException;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static sleeper.dynamodb.tools.DynamoDBAttributes.createNumberAttribute;
import static sleeper.dynamodb.tools.DynamoDBAttributes.createStringAttribute;

class DynamoDBFileLifecycleInfoFormat {
    static final String NAME = "Name";
    static final String STATUS = "Status";
    static final String LAST_UPDATE_TIME = "LastUpdateTime";

    DynamoDBFileLifecycleInfoFormat() {
    }

    Map<String, AttributeValue> createRecord(FileLifecycleInfo fileLifecycleInfo) throws StateStoreException {
        Map<String, AttributeValue> record = new HashMap<>();
        record.put(NAME, createStringAttribute(fileLifecycleInfo.getFilename()));
        record.put(STATUS, createStringAttribute(fileLifecycleInfo.getFileStatus().toString()));
        if (null != fileLifecycleInfo.getLastStateStoreUpdateTime()) {
            record.put(LAST_UPDATE_TIME, createNumberAttribute(fileLifecycleInfo.getLastStateStoreUpdateTime()));
        } else {
            record.put(LAST_UPDATE_TIME, createNumberAttribute(Instant.now().toEpochMilli()));
        }
        return record;
    }

    FileLifecycleInfo getFileLifecycleInfoFromAttributeValues(Map<String, AttributeValue> item) throws IOException {
        FileLifecycleInfo.Builder fileInfoBuilder = FileLifecycleInfo.builder()
                .filename(item.get(NAME).getS());
        fileInfoBuilder.fileStatus(FileLifecycleInfo.FileStatus.valueOf(item.get(STATUS).getS()));
        if (null != item.get(LAST_UPDATE_TIME)) {
            fileInfoBuilder.lastStateStoreUpdateTime(Long.parseLong(item.get(LAST_UPDATE_TIME).getN()));
        }
        return fileInfoBuilder.build();
    }
}

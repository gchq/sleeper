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
package sleeper.table.job;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.s3.AmazonS3;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;

import sleeper.configuration.properties.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.schema.type.ByteArrayType;
import sleeper.core.schema.type.IntType;
import sleeper.core.schema.type.LongType;
import sleeper.core.schema.type.PrimitiveType;
import sleeper.core.schema.type.StringType;
import sleeper.statestore.InitialiseStateStore;
import sleeper.statestore.StateStore;
import sleeper.statestore.StateStoreException;
import sleeper.statestore.StateStoreFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static sleeper.configuration.properties.table.TableProperty.SPLIT_POINTS_BASE64_ENCODED;
import static sleeper.configuration.properties.table.TableProperty.SPLIT_POINTS_KEY;

/**
 * The TableInitialiser is a class which can initialise a StateStore using TableProperties
 * unlike the {@link TableCreator}, this class may be used as part of main code.
 */
public class TableInitialiser {
    private AmazonS3 s3;
    private AmazonDynamoDB dynamoDB;

    public TableInitialiser(AmazonS3 s3, AmazonDynamoDB dynamoDB) {
        this.s3 = s3;
        this.dynamoDB = dynamoDB;
    }

    public void initialise(InstanceProperties instanceProperties,
                           TableProperties tableProperties,
                           String configBucket,
                           Configuration configuration) throws IOException {
        StateStore stateStore = new StateStoreFactory(dynamoDB, instanceProperties, configuration).getStateStore(tableProperties);
        List<Object> splitPoints = getSplitPoints(tableProperties, configBucket);
        try {
            InitialiseStateStore.createInitialiseStateStoreFromSplitPoints(tableProperties, stateStore, splitPoints).run();
        } catch (StateStoreException e) {
            throw new RuntimeException("Failed to initialise State Store", e);
        }
    }

    private List<Object> getSplitPoints(TableProperties tableProperties, String configBucket) {
        String splitsFile = tableProperties.get(SPLIT_POINTS_KEY);
        if (splitsFile == null) {
            return null;
        }
        List<Object> splitPoints = new ArrayList<>();
        try (InputStreamReader inputStreamReader = new InputStreamReader(s3.getObject(configBucket, splitsFile).getObjectContent(), StandardCharsets.UTF_8);
             BufferedReader bufferedReader = new BufferedReader(inputStreamReader)) {
            boolean stringsBase64Encoded = Boolean.parseBoolean(tableProperties.get(SPLIT_POINTS_BASE64_ENCODED));
            PrimitiveType keyType = tableProperties.getSchema().getRowKeyTypes().get(0);
            bufferedReader.lines()
                    .map(l -> createSplitPoint(l, stringsBase64Encoded, keyType))
                    .forEach(splitPoints::add);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return splitPoints;
    }

    private Object createSplitPoint(String line, boolean stringsBase64Encoded, PrimitiveType keyType) {
        if (keyType instanceof IntType) {
            return Integer.parseInt(line);
        }
        if (keyType instanceof LongType) {
            return Long.parseLong(line);
        }
        if (keyType instanceof StringType) {
            if (stringsBase64Encoded) {
                return new String(Base64.decodeBase64(line), StandardCharsets.UTF_8);
            }
            return line;
        }
        if (keyType instanceof ByteArrayType) {
            return Base64.decodeBase64(line);
        }
        throw new RuntimeException("Unknown PrimitiveType used for split points.");
    }
}

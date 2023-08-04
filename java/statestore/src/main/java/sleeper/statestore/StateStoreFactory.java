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
package sleeper.statestore;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import org.apache.hadoop.conf.Configuration;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.table.TableProperties;
import sleeper.core.statestore.StateStore;
import sleeper.statestore.dynamodb.DynamoDBStateStore;
import sleeper.statestore.s3.S3StateStore;

import static sleeper.configuration.properties.table.TableProperty.STATESTORE_CLASSNAME;

public class StateStoreFactory {
    private final AmazonDynamoDB dynamoDB;
    private final InstanceProperties instanceProperties;
    private final Configuration configuration;

    public StateStoreFactory(AmazonDynamoDB dynamoDB, InstanceProperties instanceProperties, Configuration configuration) {
        this.dynamoDB = dynamoDB;
        this.instanceProperties = instanceProperties;
        this.configuration = configuration;
    }

    public StateStore getStateStore(TableProperties tableProperties) {
        String stateStoreClassName = tableProperties.get(STATESTORE_CLASSNAME);
        if (stateStoreClassName.equals(DynamoDBStateStore.class.getName())) {
            return new DynamoDBStateStore(tableProperties, dynamoDB);
        }
        if (stateStoreClassName.equals(S3StateStore.class.getName())) {
            return new S3StateStore(instanceProperties, tableProperties, dynamoDB, configuration);
        }
        throw new RuntimeException("Unknown StateStore class: " + stateStoreClassName);
    }
}

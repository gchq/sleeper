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

package sleeper.systemtest.drivers.instance;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.s3.AmazonS3;

import sleeper.clients.deploy.DeployInstanceConfiguration;
import sleeper.configuration.properties.SleeperProperties;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.instance.SleeperProperty;
import sleeper.configuration.properties.table.S3TableProperties;
import sleeper.configuration.properties.table.TableProperties;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.function.Predicate.not;
import static sleeper.configuration.properties.table.TableProperty.TABLE_NAME;

public class ResetProperties {

    private ResetProperties() {
    }

    public static void reset(DeployInstanceConfiguration configuration,
                             InstanceProperties instanceProperties,
                             List<TableProperties> tableProperties,
                             AmazonS3 s3Client, AmazonDynamoDB dynamoClient) {

        reset(instanceProperties, configuration.getInstanceProperties());
        instanceProperties.saveToS3(s3Client);

        Map<String, TableProperties> configTableByName = configuration.getTableProperties().stream()
                .collect(Collectors.toMap(properties -> properties.get(TABLE_NAME), properties -> properties));
        for (TableProperties properties : tableProperties) {
            TableProperties configProperties = configTableByName.get(properties.get(TABLE_NAME));
            reset(properties, configProperties);
            S3TableProperties.getStore(instanceProperties, s3Client, dynamoClient).save(properties);
        }
    }

    private static <T extends SleeperProperty> void reset(
            SleeperProperties<T> properties,
            SleeperProperties<T> resetProperties) {
        for (T property : propertiesToReset(properties)) {
            if (resetProperties.isSet(property)) {
                properties.set(property, resetProperties.get(property));
            } else {
                properties.unset(property);
            }
        }
    }

    private static <T extends SleeperProperty> Iterable<T> propertiesToReset(SleeperProperties<T> properties) {
        return () -> properties.getPropertiesIndex().getUserDefined().stream()
                .filter(SleeperProperty::isEditable)
                .filter(not(SleeperProperty::isRunCdkDeployWhenChanged))
                .iterator();
    }
}

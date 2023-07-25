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

import com.amazonaws.services.s3.AmazonS3;

import sleeper.clients.deploy.DeployInstanceConfiguration;
import sleeper.configuration.properties.SleeperProperties;
import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.configuration.properties.instance.SleeperProperty;
import sleeper.configuration.properties.table.TableProperties;

import java.io.IOException;
import java.io.UncheckedIOException;

import static java.util.function.Predicate.not;

public class ResetProperties {

    public static void reset(DeployInstanceConfiguration configuration,
                             InstanceProperties instanceProperties,
                             TableProperties tableProperties,
                             AmazonS3 s3Client) {
        try {
            reset(instanceProperties, configuration.getInstanceProperties());
            instanceProperties.saveToS3(s3Client);
            reset(tableProperties, configuration.getTableProperties());
            tableProperties.saveToS3(s3Client);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
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
                .filter(not(SleeperProperty::isRunCDKDeployWhenChanged))
                .iterator();
    }
}

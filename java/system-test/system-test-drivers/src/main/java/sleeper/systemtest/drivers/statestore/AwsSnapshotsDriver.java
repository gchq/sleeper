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
package sleeper.systemtest.drivers.statestore;

import com.amazonaws.services.cloudwatchevents.AmazonCloudWatchEvents;
import com.amazonaws.services.cloudwatchevents.model.DisableRuleRequest;
import com.amazonaws.services.cloudwatchevents.model.EnableRuleRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.systemtest.dsl.snapshot.SnapshotsDriver;

import static sleeper.configuration.properties.instance.CdkDefinedInstanceProperty.TRANSACTION_LOG_SNAPSHOT_CREATION_RULE;

public class AwsSnapshotsDriver implements SnapshotsDriver {
    public static final Logger LOGGER = LoggerFactory.getLogger(AwsSnapshotsDriver.class);
    private final AmazonCloudWatchEvents cwClient;

    public AwsSnapshotsDriver(AmazonCloudWatchEvents cwClient) {
        this.cwClient = cwClient;
    }

    @Override
    public void enableCreation(InstanceProperties instanceProperties) {
        LOGGER.info("Enabling transaction log snapshot creation");
        cwClient.enableRule(new EnableRuleRequest()
                .withName(instanceProperties.get(TRANSACTION_LOG_SNAPSHOT_CREATION_RULE)));
    }

    @Override
    public void disableCreation(InstanceProperties instanceProperties) {
        LOGGER.info("Disabling transaction log snapshot creation");
        cwClient.disableRule(new DisableRuleRequest()
                .withName(instanceProperties.get(TRANSACTION_LOG_SNAPSHOT_CREATION_RULE)));
    }
}

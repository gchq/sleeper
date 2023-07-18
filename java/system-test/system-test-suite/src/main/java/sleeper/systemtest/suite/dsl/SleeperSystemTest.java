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

package sleeper.systemtest.suite.dsl;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import software.amazon.awssdk.services.cloudformation.CloudFormationClient;

import sleeper.configuration.properties.instance.InstanceProperties;
import sleeper.systemtest.drivers.instance.SleeperInstanceContext;
import sleeper.systemtest.drivers.instance.SystemTestParameters;
import sleeper.systemtest.suite.fixtures.SystemTestInstance;

public class SleeperSystemTest {

    private static final SleeperSystemTest INSTANCE = new SleeperSystemTest();

    private final SystemTestParameters parameters = SystemTestParameters.loadFromSystemProperties();
    private final CloudFormationClient cloudFormationClient = CloudFormationClient.create();
    private final AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
    private final SleeperInstanceContext instance = new SleeperInstanceContext(parameters, cloudFormationClient, s3Client);

    public static SleeperSystemTest getInstance() {
        return INSTANCE;
    }

    public void connectToInstance(SystemTestInstance testInstance) {
        instance.connectTo(testInstance.getIdentifier(), testInstance.getInstanceConfiguration());
    }

    public InstanceProperties instanceProperties() {
        return instance.getCurrentInstance().getInstanceProperties();
    }
}

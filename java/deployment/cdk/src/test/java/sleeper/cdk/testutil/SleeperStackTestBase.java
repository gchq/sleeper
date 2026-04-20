/*
 * Copyright 2022-2026 Crown Copyright
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
package sleeper.cdk.testutil;

import org.junit.jupiter.api.BeforeEach;
import software.amazon.awscdk.App;
import software.amazon.awscdk.AppProps;
import software.amazon.awscdk.Environment;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.StackProps;

import sleeper.cdk.SleeperInstanceProps;
import sleeper.cdk.artefacts.SleeperArtefacts;
import sleeper.cdk.artefacts.SleeperInstanceArtefacts;
import sleeper.cdk.artefacts.containers.SleeperContainerImageDigestProvider;
import sleeper.cdk.artefacts.jars.SleeperJarVersionIdProvider;
import sleeper.core.properties.instance.InstanceProperties;

import static sleeper.core.properties.instance.CommonProperty.ID;
import static sleeper.core.properties.instance.CommonProperty.JARS_BUCKET;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstancePropertiesWithId;

public class SleeperStackTestBase {

    protected final InstanceProperties instanceProperties = createTestInstancePropertiesWithId("test-instance");
    protected final SleeperInstancePrinter printer = new SleeperInstancePrinter();
    protected final Stack rootStack = createRootStack();
    private final SleeperJarVersionIdProvider jarVersionIdProvider = new SleeperJarVersionIdProvider(jar -> jar.getArtifactId() + "-test-version");
    private final SleeperContainerImageDigestProvider imageDigestProvider = new SleeperContainerImageDigestProvider((image, ecrRepository) -> image + "-test-digest");
    private final SleeperArtefacts artefacts = SleeperArtefacts.fromProperties(jarVersionIdProvider, imageDigestProvider);

    @BeforeEach
    void setUpBase() {
        instanceProperties.unset(JARS_BUCKET);
    }

    protected SleeperInstanceProps instanceProps() {
        return SleeperInstanceProps.builder()
                .instanceProperties(instanceProperties)
                .version("1.2.3")
                .artefacts(artefacts())
                .skipCheckingVersionMatchesProperties(true)
                .build();
    }

    protected SleeperInstanceArtefacts instanceArtefacts() {
        return artefacts.forInstance(instanceProperties);
    }

    protected SleeperArtefacts artefacts() {
        return artefacts;
    }

    private Stack createRootStack() {
        App app = new App(AppProps.builder()
                .analyticsReporting(false)
                .build());
        Environment environment = Environment.builder()
                .account("test-account")
                .region("test-region")
                .build();
        StackProps stackProps = StackProps.builder()
                .stackName(instanceProperties.get(ID))
                .env(environment)
                .build();
        return new Stack(app, "TestInstance", stackProps);
    }

}

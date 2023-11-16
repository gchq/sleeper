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
package sleeper.environment.cdk;

import software.amazon.awscdk.App;
import software.amazon.awscdk.Environment;
import software.amazon.awscdk.StackProps;

import sleeper.environment.cdk.buildec2.BuildEC2Stack;
import sleeper.environment.cdk.config.AppContext;
import sleeper.environment.cdk.networking.NetworkingStack;

import static sleeper.environment.cdk.config.AppParameters.INSTANCE_ID;

/**
 * The {@link App} that sets up an environment for deploying Sleeper.
 */
public class SleeperEnvironmentCdkApp {

    private SleeperEnvironmentCdkApp() {
    }

    public static void main(String[] args) {
        App app = new App();
        Environment environment = Environment.builder()
                .account(System.getenv("CDK_DEFAULT_ACCOUNT"))
                .region(System.getenv("CDK_DEFAULT_REGION"))
                .build();
        String instanceId = AppContext.of(app).get(INSTANCE_ID);
        NetworkingStack networking = new NetworkingStack(app,
                StackProps.builder().stackName(instanceId + "-Networking").env(environment).analyticsReporting(false).build());
        new BuildEC2Stack(app,
                StackProps.builder().stackName(instanceId + "-BuildEC2").env(environment).analyticsReporting(false).build(),
                networking.getVpc());
        app.synth();
    }
}

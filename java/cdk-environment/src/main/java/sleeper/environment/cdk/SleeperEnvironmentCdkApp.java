/*
 * Copyright 2022 Crown Copyright
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

import sleeper.environment.cdk.buildec2.BuildEC2Stack;
import sleeper.environment.cdk.networking.NetworkingStack;
import sleeper.environment.cdk.util.AppContext;
import software.amazon.awscdk.App;
import software.amazon.awscdk.Environment;
import software.amazon.awscdk.StackProps;

/**
 * The {@link App} that sets up an environment for deploying Sleeper.
 */
public class SleeperEnvironmentCdkApp {

    public static void main(String[] args) {
        App app = new App();
        Environment environment = Environment.builder()
                .account(System.getenv("CDK_DEFAULT_ACCOUNT"))
                .region(System.getenv("CDK_DEFAULT_REGION"))
                .build();
        AppContext context = AppContext.of(app);
        NetworkingStack networking = new NetworkingStack(app,
                StackProps.builder().stackName(context.getInstanceId() + "-Networking").env(environment).build());
        new BuildEC2Stack(app,
                StackProps.builder().stackName(context.getInstanceId() + "-BuildEC2").env(environment).build(),
                networking.getVpc());
        app.synth();
    }
}
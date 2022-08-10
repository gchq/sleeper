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
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.StackProps;
import software.constructs.Construct;

/**
 * The {@link App} that sets up an environment for deploying Sleeper.
 */
public class SleeperEnvironmentCdkApp extends Stack {

    public SleeperEnvironmentCdkApp(Construct scope, StackProps props, AppContext app) {
        super(scope, props.getStackName(), props);

        NetworkingStack networking = new NetworkingStack(this);
        new BuildEC2Stack(this, networking.getVpc(), app);
    }

    public static void main(String[] args) {
        App app = new App();
        StackProps props = StackProps.builder()
                .stackName("SleeperEnvironment")
                .env(Environment.builder()
                        .account(System.getenv("CDK_DEFAULT_ACCOUNT"))
                        .region(System.getenv("CDK_DEFAULT_REGION"))
                        .build())
                .build();
        new SleeperEnvironmentCdkApp(app, props, AppContext.of(app));
        app.synth();
    }
}
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
package sleeper.environment.cdk;

import software.amazon.awscdk.App;
import software.amazon.awscdk.AppProps;
import software.amazon.awscdk.Environment;
import software.amazon.awscdk.StackProps;

import sleeper.environment.cdk.buildec2.BuildEC2Stack;
import sleeper.environment.cdk.builduptime.BuildUptimeStack;
import sleeper.environment.cdk.config.AppContext;
import sleeper.environment.cdk.networking.NetworkingStack;

import static sleeper.environment.cdk.config.AppParameters.BUILD_UPTIME_LAMBDA_JAR;
import static sleeper.environment.cdk.config.AppParameters.INSTANCE_ID;

/**
 * Deploys an environment suitable for Sleeper, including a VPC and an EC2 instance to run the deployment from.
 */
public class SleeperEnvironmentCdkApp {

    private SleeperEnvironmentCdkApp() {
    }

    public static void main(String[] args) {
        App app = new App(AppProps.builder()
                .analyticsReporting(false)
                .build());
        Environment environment = Environment.builder()
                .account(System.getenv("CDK_DEFAULT_ACCOUNT"))
                .region(System.getenv("CDK_DEFAULT_REGION"))
                .build();
        AppContext context = AppContext.of(app);
        String instanceId = context.get(INSTANCE_ID);
        NetworkingStack networking = new NetworkingStack(app,
                StackProps.builder().stackName(instanceId + "-Networking").env(environment).build());
        BuildEC2Stack buildEc2 = new BuildEC2Stack(app,
                StackProps.builder().stackName(instanceId + "-BuildEC2").env(environment).build(),
                networking.getVpc());
        if (context.get(BUILD_UPTIME_LAMBDA_JAR).isPresent()) {
            new BuildUptimeStack(app,
                    StackProps.builder().stackName(instanceId + "-BuildUptime").env(environment).build(),
                    buildEc2.getInstance());
        }
        app.synth();
    }
}

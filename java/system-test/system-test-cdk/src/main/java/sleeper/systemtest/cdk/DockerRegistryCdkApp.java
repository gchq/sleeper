/*
 * Copyright 2022-2025 Crown Copyright
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
package sleeper.systemtest.cdk;

import software.amazon.awscdk.App;
import software.amazon.awscdk.AppProps;
import software.amazon.awscdk.Environment;
import software.amazon.awscdk.RemovalPolicy;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.StackProps;
import software.amazon.awscdk.services.ec2.IVpc;
import software.amazon.awscdk.services.ec2.SubnetSelection;
import software.amazon.awscdk.services.ec2.SubnetType;
import software.amazon.awscdk.services.ec2.Vpc;
import software.amazon.awscdk.services.ec2.VpcLookupOptions;
import software.amazon.awscdk.services.ecs.Cluster;
import software.amazon.awscdk.services.ecs.ContainerDefinitionOptions;
import software.amazon.awscdk.services.ecs.ContainerImage;
import software.amazon.awscdk.services.ecs.ContainerInsights;
import software.amazon.awscdk.services.ecs.FargateService;
import software.amazon.awscdk.services.ecs.FargateTaskDefinition;
import software.amazon.awscdk.services.logs.LogGroup;
import software.amazon.awscdk.services.logs.RetentionDays;
import software.constructs.Construct;

import sleeper.cdk.util.CdkContext;
import sleeper.cdk.util.Utils;

/**
 * A CDK app to deploy a Docker registry within a VPC for testing.
 */
public class DockerRegistryCdkApp extends Stack {

    public DockerRegistryCdkApp(Construct scope, String id, StackProps stackProps, String vpcId) {
        super(scope, id, stackProps);

        IVpc vpc = Vpc.fromLookup(this, "Vpc", VpcLookupOptions.builder().vpcId(vpcId).build());
        Cluster cluster = Cluster.Builder
                .create(this, "Cluster")
                .clusterName(id)
                .containerInsightsV2(ContainerInsights.ENHANCED)
                .vpc(vpc)
                .build();
        FargateTaskDefinition taskDefinition = FargateTaskDefinition.Builder.create(this, "Task")
                .family(id)
                .cpu(256)
                .memoryLimitMiB(512)
                .build();
        taskDefinition.addContainer("Registry", ContainerDefinitionOptions.builder()
                .image(ContainerImage.fromRegistry("registry"))
                .logging(Utils.createECSContainerLogDriver(LogGroup.Builder.create(this, "LogGroup")
                        .logGroupName(id + "-container")
                        .retention(RetentionDays.TWO_WEEKS)
                        .removalPolicy(RemovalPolicy.DESTROY)
                        .build()))
                .build());
        FargateService.Builder.create(this, "Service")
                .cluster(cluster).serviceName(id)
                .taskDefinition(taskDefinition)
                .desiredCount(1)
                .vpcSubnets(SubnetSelection.builder()
                        .subnetType(SubnetType.PRIVATE_WITH_EGRESS)
                        .build())
                .build();
    }

    public static void main(String[] args) {
        App app = new App(AppProps.builder()
                .analyticsReporting(false)
                .build());
        CdkContext context = CdkContext.from(app);
        String id = context.tryGetContext("id");
        Environment environment = Environment.builder()
                .account(System.getenv("CDK_DEFAULT_ACCOUNT"))
                .region(System.getenv("CDK_DEFAULT_REGION"))
                .build();

        new DockerRegistryCdkApp(app, id,
                StackProps.builder().env(environment).build(),
                context.tryGetContext("vpc"));

        app.synth();
    }
}

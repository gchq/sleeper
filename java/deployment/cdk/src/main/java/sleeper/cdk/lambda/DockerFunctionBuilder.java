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
package sleeper.cdk.lambda;

import software.amazon.awscdk.Duration;
import software.amazon.awscdk.services.ec2.ISecurityGroup;
import software.amazon.awscdk.services.ec2.IVpc;
import software.amazon.awscdk.services.ec2.SubnetSelection;
import software.amazon.awscdk.services.lambda.DockerImageFunction;
import software.amazon.awscdk.services.lambda.DockerImageFunction.Builder;
import software.amazon.awscdk.services.lambda.Function;
import software.amazon.awscdk.services.lambda.IEventSource;
import software.amazon.awscdk.services.logs.ILogGroup;

import java.util.List;
import java.util.Map;

public class DockerFunctionBuilder implements LambdaBuilder {

    private final DockerImageFunction.Builder builder;

    public DockerFunctionBuilder(Builder builder) {
        this.builder = builder;
    }

    @Override
    public LambdaBuilder functionName(String functionName) {
        builder.functionName(functionName);
        return this;
    }

    @Override
    public LambdaBuilder description(String description) {
        builder.description(description);
        return this;
    }

    @Override
    public LambdaBuilder memorySize(Number memorySize) {
        builder.memorySize(memorySize);
        return this;
    }

    @Override
    public LambdaBuilder timeout(Duration timeout) {
        builder.timeout(timeout);
        return this;
    }

    @Override
    public LambdaBuilder environment(Map<String, String> environment) {
        builder.environment(environment);
        return this;
    }

    @Override
    public LambdaBuilder logGroup(ILogGroup logGroup) {
        builder.logGroup(logGroup);
        return this;
    }

    @Override
    public LambdaBuilder events(List<IEventSource> events) {
        builder.events(events);
        return this;
    }

    @Override
    public LambdaBuilder reservedConcurrentExecutions(Number reservedConcurrentExecutions) {
        builder.reservedConcurrentExecutions(reservedConcurrentExecutions);
        return this;
    }

    @Override
    public LambdaBuilder vpc(IVpc vpc) {
        builder.vpc(vpc);
        return this;
    }

    @Override
    public LambdaBuilder vpcSubnets(SubnetSelection vpcSubnets) {
        builder.vpcSubnets(vpcSubnets);
        return this;
    }

    @Override
    public LambdaBuilder securityGroups(List<? extends ISecurityGroup> securityGroups) {
        builder.securityGroups(securityGroups);
        return this;
    }

    @Override
    public Function build() {
        return builder.build();
    }
}

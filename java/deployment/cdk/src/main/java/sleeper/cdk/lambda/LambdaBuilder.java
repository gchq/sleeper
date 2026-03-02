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
import software.amazon.awscdk.services.lambda.Function;
import software.amazon.awscdk.services.lambda.IEventSource;
import software.amazon.awscdk.services.logs.ILogGroup;

import java.util.List;
import java.util.Map;

public interface LambdaBuilder {

    /**
     * Sets the function name. See
     * {@link software.amazon.awscdk.services.lambda.Function.Builder#functionName(String)}.
     *
     * @param  functionName the function name
     * @return              this builder
     */
    LambdaBuilder functionName(String functionName);

    /**
     * Sets the description. See
     * {@link software.amazon.awscdk.services.lambda.Function.Builder#description(String)}.
     *
     * @param  description the description
     * @return             this builder
     */
    LambdaBuilder description(String description);

    /**
     * Sets the amount of memory in MB allocated to the function. See
     * {@link software.amazon.awscdk.services.lambda.Function.Builder#memorySize(Number)}.
     *
     * @param  memorySize the amount of memory in MB
     * @return            this builder
     */
    LambdaBuilder memorySize(Number memorySize);

    /**
     * Sets the timeout for the lambda. See
     * {@link software.amazon.awscdk.services.lambda.Function.Builder#timeout(Duration)}.
     *
     * @param  timeout the timeout
     * @return         this builder
     */
    LambdaBuilder timeout(Duration timeout);

    /**
     * Sets the environment variables. See
     * {@link software.amazon.awscdk.services.lambda.Function.Builder#environment(Map)}.
     *
     * @param  environment the environment
     * @return             this builder
     */
    LambdaBuilder environment(Map<String, String> environment);

    /**
     * Sets the log group. See
     * {@link software.amazon.awscdk.services.lambda.Function.Builder#logGroup(ILogGroup)}.
     *
     * @param  logGroup the log group
     * @return          this builder
     */
    LambdaBuilder logGroup(ILogGroup logGroup);

    /**
     * Sets the event sources that will invoke the function. See
     * {@link software.amazon.awscdk.services.lambda.Function.Builder#events(List)}.
     *
     * @param  events the event sources
     * @return        this builder
     */
    LambdaBuilder events(List<IEventSource> events);

    /**
     * Sets the reserved concurrent executions. See
     * {@link software.amazon.awscdk.services.lambda.Function.Builder#reservedConcurrentExecutions(Number)}.
     *
     * @param  reservedConcurrentExecutions the reserved concurrent executions
     * @return                              this builder
     */
    LambdaBuilder reservedConcurrentExecutions(Number reservedConcurrentExecutions);

    /**
     * Sets a VPC to attach the lambda to. See
     * {@link software.amazon.awscdk.services.lambda.Function.Builder#vpc(IVpc)}.
     *
     * @param  vpc the VPC
     * @return     this builder
     */
    LambdaBuilder vpc(IVpc vpc);

    /**
     * Sets the subnets in the VPC to attach the lambda to. See
     * {@link software.amazon.awscdk.services.lambda.Function.Builder#vpcSubnets(SubnetSelection)}.
     *
     * @param  vpcSubnets the subnets
     * @return            this builder
     */
    LambdaBuilder vpcSubnets(SubnetSelection vpcSubnets);

    /**
     * Sets the security groups for the lambda within the VPC. See
     * {@link software.amazon.awscdk.services.lambda.Function.Builder#securityGroups(List)}.
     *
     * @param  securityGroups the security groups
     * @return                this builder
     */
    LambdaBuilder securityGroups(List<? extends ISecurityGroup> securityGroups);

    /**
     * Creates the lambda function.
     *
     * @return the function
     */
    Function build();

}

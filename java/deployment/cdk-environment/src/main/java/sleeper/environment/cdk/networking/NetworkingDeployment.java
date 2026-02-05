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
package sleeper.environment.cdk.networking;

import software.amazon.awscdk.services.ec2.GatewayVpcEndpoint;
import software.amazon.awscdk.services.ec2.GatewayVpcEndpointAwsService;
import software.amazon.awscdk.services.ec2.IVpc;
import software.amazon.awscdk.services.ec2.IpAddresses;
import software.amazon.awscdk.services.ec2.SubnetConfiguration;
import software.amazon.awscdk.services.ec2.SubnetSelection;
import software.amazon.awscdk.services.ec2.SubnetType;
import software.amazon.awscdk.services.ec2.Vpc;
import software.amazon.awscdk.services.ec2.VpcLookupOptions;
import software.constructs.Construct;

import sleeper.environment.cdk.config.AppContext;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import static sleeper.environment.cdk.config.AppParameters.VPC_ID;

public class NetworkingDeployment {

    private final IVpc vpc;

    public NetworkingDeployment(Construct scope) {
        AppContext context = AppContext.of(scope);
        Optional<String> vpcId = context.get(VPC_ID);
        if (vpcId.isPresent()) {
            vpc = Vpc.fromLookup(scope, "Vpc", VpcLookupOptions.builder().vpcId(vpcId.get()).build());
            return;
        }
        vpc = Vpc.Builder.create(scope, "Vpc")
                .ipAddresses(IpAddresses.cidr("10.0.0.0/16"))
                .maxAzs(3)
                .natGateways(1)
                .restrictDefaultSecurityGroup(true)
                .subnetConfiguration(Arrays.asList(
                        SubnetConfiguration.builder().name("public")
                                .subnetType(SubnetType.PUBLIC)
                                .cidrMask(26).build(),
                        SubnetConfiguration.builder().name("private")
                                .subnetType(SubnetType.PRIVATE_WITH_EGRESS)
                                .cidrMask(19).build()))
                .build();

        GatewayVpcEndpoint.Builder.create(scope, "S3Endpoint").vpc(vpc)
                .service(GatewayVpcEndpointAwsService.S3)
                .subnets(Collections.singletonList(SubnetSelection.builder()
                        .subnetType(SubnetType.PRIVATE_WITH_EGRESS).build()))
                .build();

        GatewayVpcEndpoint.Builder.create(scope, "DynamoDBEndpoint").vpc(vpc)
                .service(GatewayVpcEndpointAwsService.DYNAMODB)
                .subnets(Collections.singletonList(SubnetSelection.builder()
                        .subnetType(SubnetType.PRIVATE_WITH_EGRESS).build()))
                .build();
    }

    public IVpc getVpc() {
        return vpc;
    }
}

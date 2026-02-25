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
package sleeper.cdk.networking;

import software.amazon.awscdk.Stack;
import software.amazon.awscdk.services.ec2.ISubnet;
import software.amazon.awscdk.services.ec2.IVpc;
import software.amazon.awscdk.services.ec2.Subnet;
import software.amazon.awscdk.services.ec2.Vpc;
import software.amazon.awscdk.services.ec2.VpcLookupOptions;
import software.constructs.Construct;

import sleeper.cdk.util.CdkContext;
import sleeper.core.properties.instance.InstanceProperties;

import java.util.List;
import java.util.stream.IntStream;

import static sleeper.core.properties.instance.CommonProperty.SUBNETS;
import static sleeper.core.properties.instance.CommonProperty.VPC_ID;

/**
 * Networking settings to deploy an instance of Sleeper. To use all private subnets you can pass in the result of
 * {@link IVpc#getPrivateSubnets() }.
 *
 * @param vpc     the VPC to deploy Sleeper into
 * @param subnets the subnets to deploy Sleeper into
 */
public record SleeperNetworking(IVpc vpc, List<ISubnet> subnets) {

    /**
     * Refers to VPCs and subnets by their IDs. First reads the CDK context variables "vpc" and "subnets". If the VPC
     * is set but the subnets are not, defaults to all private subnets. If the context variables are not set, reads the
     * IDs from the instance properties.
     *
     * @param  scope              the scope to add references to the VPC and subnets to
     * @param  context            the context to read the IDs from if they are set
     * @param  instanceProperties the configuration to read the IDs from if the context variables are not set
     * @return                    the networking settings
     */
    public static SleeperNetworking createByContext(Construct scope, CdkContext context, InstanceProperties instanceProperties) {
        String vpcId = context.tryGetContext("vpc");
        if (vpcId != null) {
            List<String> subnetIds = context.getList("subnets");
            return createByIds(scope, vpcId, subnetIds);
        } else {
            return createByProperties(scope, instanceProperties);
        }
    }

    /**
     * Refers to VPC and subnets by their IDs.
     *
     * @param  scope      the scope to add references to the VPC and subnets to
     * @param  properties the Sleeper instance properties
     * @return            the networking settings
     */
    public static SleeperNetworking createByProperties(Construct scope, InstanceProperties properties) {
        return createByIds(scope, properties.get(VPC_ID), properties.getList(SUBNETS));
    }

    /**
     * Refers to VPC and subnets by their IDs.
     *
     * @param  scope     the scope to add references to the VPC and subnets to
     * @param  vpcId     the VPC ID
     * @param  subnetIds the subnet IDs
     * @return           the networking settings
     */
    public static SleeperNetworking createByIds(Construct scope, String vpcId, List<String> subnetIds) {
        IVpc vpc = Vpc.fromLookup(scope, "VPC", VpcLookupOptions.builder()
                .vpcId(vpcId)
                .build());
        List<ISubnet> subnets = subnetIds == null
                ? vpc.getPrivateSubnets()
                : IntStream.range(0, subnetIds.size())
                        .mapToObj(i -> Subnet.fromSubnetId(scope, "Subnet" + i, subnetIds.get(i)))
                        .toList();
        return new SleeperNetworking(vpc, subnets);
    }

    public String vpcId() {
        return vpc.getVpcId();
    }

    public String vpcArn() {
        return vpc.getVpcArn();
    }

    public List<String> subnetIds() {
        return subnets.stream().map(ISubnet::getSubnetId).toList();
    }

    public List<String> subnetArns() {
        return subnets.stream().map(SleeperNetworking::getSubnetArn).toList();
    }

    private static String getSubnetArn(ISubnet subnet) {
        Stack stack = subnet.getStack();
        String partition = stack.getPartition();
        String region = stack.getRegion();
        String account = stack.getAccount();
        String subnetId = subnet.getSubnetId();
        return "arn:" + partition + ":ec2:" + region + ":" + account + ":subnet/" + subnetId;
    }
}

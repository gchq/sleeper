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

import sleeper.core.properties.instance.InstanceProperties;

import java.util.ArrayList;
import java.util.List;

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
     * Refers to VPCs and subnets by their IDs.
     *
     * @param  properties the Sleeper instance properties
     * @return            the networking settings
     */
    public static SleeperNetworking createByProperties(Construct scope, InstanceProperties properties) {
        return createByIds(scope, properties.get(VPC_ID), properties.getList(SUBNETS));
    }

    /**
     * Refers to VPCs and subnets by their IDs.
     *
     * @param  vpcId     the VPC ID
     * @param  subnetIds the subnet IDs
     * @return           the networking settings
     */
    public static SleeperNetworking createByIds(Construct scope, String vpcId, List<String> subnetIds) {
        IVpc vpc = Vpc.fromLookup(scope, "VPC", VpcLookupOptions.builder()
                .vpcId(vpcId)
                .build());
        List<ISubnet> subnets = new ArrayList<>(subnetIds.size());
        for (int i = 0; i < subnetIds.size(); i++) {
            subnets.add(Subnet.fromSubnetId(scope, "Subnet" + i, subnetIds.get(i)));
        }
        return new SleeperNetworking(vpc, subnets);
    }

    public static String getSubnetArn(ISubnet subnet) {
        Stack stack = subnet.getStack();
        String partition = stack.getPartition();
        String region = stack.getRegion();
        String account = stack.getAccount();
        String subnetId = subnet.getSubnetId();
        return "arn:" + partition + ":ec2:" + region + ":" + account + ":subnet/" + subnetId;
    }
}

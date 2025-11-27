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

import software.amazon.awscdk.services.ec2.ISubnet;
import software.amazon.awscdk.services.ec2.IVpc;
import software.amazon.awscdk.services.ec2.Subnet;
import software.amazon.awscdk.services.ec2.Vpc;
import software.amazon.awscdk.services.ec2.VpcLookupOptions;
import software.constructs.Construct;

import java.util.ArrayList;
import java.util.List;

/**
 * Networking settings to deploy an instance of Sleeper.
 *
 * @param vpc     the VPC to deploy Sleeper into
 * @param subnets the subnets to deploy Sleeper into
 */
public record SleeperNetworking(IVpc vpc, List<ISubnet> subnets) {

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
}

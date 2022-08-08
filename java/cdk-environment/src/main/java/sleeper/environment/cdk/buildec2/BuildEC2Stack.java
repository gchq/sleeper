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
package sleeper.environment.cdk.buildec2;

import sleeper.environment.cdk.util.AppContext;
import sleeper.environment.cdk.util.MyIpUtil;
import software.amazon.awscdk.CfnOutput;
import software.amazon.awscdk.NestedStack;
import software.amazon.awscdk.services.ec2.*;
import software.constructs.Construct;

import java.util.Collections;

public class BuildEC2Stack extends NestedStack {

    public BuildEC2Stack(Construct scope, IVpc vpc, AppContext app) {
        super(scope, "BuildEC2");
        BuildEC2Params params = BuildEC2Params.from(app);

        SecurityGroup allowSsh = SecurityGroup.Builder.create(this, "AllowSsh")
                .vpc(vpc)
                .description("Allow SSH inbound traffic")
                .allowAllOutbound(true)
                .build();
        allowSsh.addIngressRule(Peer.ipv4(MyIpUtil.findMyIp() + "/32"), Port.tcp(22));

        Instance instance = Instance.Builder.create(this, "EC2")
                .vpc(vpc)
                .securityGroup(allowSsh)
                .machineImage(MachineImage.lookup(LookupMachineImageProps.builder()
                        .name("ubuntu/images/hvm-ssd/ubuntu-focal-20.04-amd64-server-*")
                        .owners(Collections.singletonList("099720109477"))
                        .build()))
                .instanceType(InstanceType.of(InstanceClass.T3, InstanceSize.LARGE))
                .vpcSubnets(SubnetSelection.builder().subnetType(SubnetType.PUBLIC).build())
                .userData(UserData.custom(LoadUserDataUtil.base64(params)))
                .userDataCausesReplacement(true)
                .build();

        CfnOutput.Builder.create(this, "ConnectCommand")
                .value("ssh ubuntu@" + instance.getInstancePublicIp())
                .description("Command to connect to EC2")
                .exportName("connectCommand")
                .build();
    }

}

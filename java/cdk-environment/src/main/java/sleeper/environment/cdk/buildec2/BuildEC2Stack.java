/*
 * Copyright 2022-2023 Crown Copyright
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

import software.amazon.awscdk.CfnOutput;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.StackProps;
import software.amazon.awscdk.services.ec2.CfnKeyPair;
import software.amazon.awscdk.services.ec2.IVpc;
import software.amazon.awscdk.services.ec2.Instance;
import software.amazon.awscdk.services.ec2.InstanceClass;
import software.amazon.awscdk.services.ec2.InstanceSize;
import software.amazon.awscdk.services.ec2.InstanceType;
import software.amazon.awscdk.services.ec2.Peer;
import software.amazon.awscdk.services.ec2.Port;
import software.amazon.awscdk.services.ec2.SecurityGroup;
import software.amazon.awscdk.services.ec2.SubnetSelection;
import software.amazon.awscdk.services.ec2.SubnetType;
import software.amazon.awscdk.services.ec2.UserData;
import software.amazon.awscdk.services.ec2.Vpc;
import software.amazon.awscdk.services.ec2.VpcLookupOptions;
import software.amazon.awscdk.services.iam.ManagedPolicy;
import software.constructs.Construct;

import sleeper.environment.cdk.config.AppContext;
import sleeper.environment.cdk.util.MyIpUtil;

import java.security.KeyPair;
import java.util.Collections;
import java.util.UUID;

import static sleeper.environment.cdk.config.AppParameters.VPC_ID;

public class BuildEC2Stack extends Stack {

    private final IVpc vpc;

    public BuildEC2Stack(Construct scope, StackProps props, IVpc inheritVpc) {
        super(scope, props.getStackName(), props);
        AppContext context = AppContext.of(this);
        BuildEC2Parameters params = BuildEC2Parameters.from(context);
        vpc = context.get(VPC_ID)
                .map(vpcId -> Vpc.fromLookup(this, "Vpc", VpcLookupOptions.builder().vpcId(vpcId).build()))
                .orElse(inheritVpc);
        BuildEC2Image image = params.image();

        String keyFile = props.getStackName() + ".pem";
        CfnKeyPair key = createSshKeyPair(keyFile);
        SecurityGroup allowSsh = createAllowSshSecurityGroup();

        Instance instance = Instance.Builder.create(this, "EC2")
                .vpc(vpc)
                .securityGroup(allowSsh)
                .machineImage(image.machineImage())
                .instanceType(InstanceType.of(InstanceClass.T3, InstanceSize.LARGE))
                .vpcSubnets(SubnetSelection.builder().subnetType(SubnetType.PUBLIC).build())
                .userData(UserData.custom(LoadUserDataUtil.userData(params)))
                .userDataCausesReplacement(true)
                .keyName(key.getKeyName())
                .blockDevices(Collections.singletonList(image.rootBlockDevice()))
                .build();
        instance.getRole().addManagedPolicy(ManagedPolicy.fromAwsManagedPolicyName("AdministratorAccess"));
        instance.getInstance().addDependency(key);

        CfnOutput.Builder.create(this, "ConnectCommand")
                .value("ssh -i " + keyFile + " " + image.loginUser() + "@" + instance.getInstancePublicIp())
                .description("Command to connect to EC2")
                .build();
        CfnOutput.Builder.create(this, "PublicIP")
                .value(instance.getInstancePublicIp())
                .description("Public IP for build EC2 instance")
                .build();
        CfnOutput.Builder.create(this, "LoginUser")
                .value(image.loginUser())
                .description("User to SSH into on build EC2 instance")
                .build();
        CfnOutput.Builder.create(this, "InstanceId")
                .value(instance.getInstanceId())
                .description("ID of the build EC2 instance")
                .build();
    }

    private CfnKeyPair createSshKeyPair(String keyFile) {
        // Create a new SSH key every time the CDK is run.
        // A UUID is appended to the key name to ensure that the key is deleted and recreated every time the stack is
        // deployed. This is because AWS does not permit updating key pairs in-place, and the library we're using does
        // not handle that.
        KeyPair keyPair = KeyPairUtil.generate();
        KeyPairUtil.writePrivateToFile(keyPair, keyFile);
        return CfnKeyPair.Builder.create(this, "KeyPair")
                .keyName(getStackName() + "-" + UUID.randomUUID())
                .publicKeyMaterial(KeyPairUtil.publicBase64(keyPair))
                .build();
    }

    private SecurityGroup createAllowSshSecurityGroup() {
        SecurityGroup allowSsh = SecurityGroup.Builder.create(this, "AllowSsh")
                .vpc(vpc)
                .description("Allow SSH inbound traffic")
                .allowAllOutbound(true)
                .build();
        allowSsh.addIngressRule(Peer.ipv4(MyIpUtil.findMyIp() + "/32"), Port.tcp(22));
        return allowSsh;
    }

}

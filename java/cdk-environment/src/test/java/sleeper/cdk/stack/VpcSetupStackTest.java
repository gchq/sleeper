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
package sleeper.cdk.stack;

import org.junit.Test;
import sleeper.environment.cdk.stack.VpcSetupStack;
import software.amazon.awscdk.App;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.assertions.Template;
import software.amazon.awscdk.services.ec2.IVpc;
import software.amazon.awscdk.services.ec2.Vpc;
import software.amazon.awscdk.services.ec2.VpcAttributes;

import java.util.Collections;


public class VpcSetupStackTest {

    @Test
    public void can_build_template_for_vpc_setup() {
        App app = new App();
        VpcSetupStack stack = new VpcSetupStack(app, testVpc(app));

        Template template = Template.fromStack(stack);

        template.resourceCountIs("AWS::EC2::VPCEndpoint", 7);
        template.hasResourceProperties("AWS::EC2::VPCEndpoint",
                Collections.singletonMap("ServiceName", "com.amazonaws.eu-west-2.s3"));
    }

    private IVpc testVpc(App app) {
        Stack stack = new Stack(app);
        return Vpc.fromVpcAttributes(stack, "Test VPC",
                VpcAttributes.builder()
                        .vpcId("TestVpc")
                        .vpcCidrBlock("0.0.0.0/0")
                        .publicSubnetIds(Collections.singletonList("subnet"))
                        .availabilityZones(Collections.singletonList("subnet")).build());
    }

}

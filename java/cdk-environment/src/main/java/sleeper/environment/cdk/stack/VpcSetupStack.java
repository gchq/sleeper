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
package sleeper.environment.cdk.stack;

import software.amazon.awscdk.Stack;
import software.amazon.awscdk.StackProps;
import software.amazon.awscdk.services.ec2.*;
import software.constructs.Construct;

public class VpcSetupStack extends Stack {

    private final IVpc vpc;

    public VpcSetupStack(Construct scope, StackProps props) {
        super(scope, "VpcSetup", props);
        vpc = Vpc.fromLookup(this, "VPC", VpcLookupOptions.builder().isDefault(true).build());
        create();
    }

    public VpcSetupStack(Construct scope, IVpc vpc) {
        super(scope, "VpcSetup", null);
        this.vpc = vpc;
        create();
    }

    private void create() {
        createEndpointByIdAndService("EC2", "com.amazonaws.eu-west-2.ec2");
        createEndpointByIdAndService("Logs", "com.amazonaws.eu-west-2.logs");
        createEndpointByIdAndService("ECR API", "com.amazonaws.eu-west-2.ecr.api");
        createEndpointByIdAndService("ECR DKR", "com.amazonaws.eu-west-2.ecr.dkr");
        createEndpointByIdAndService("STS", "com.amazonaws.eu-west-2.sts");
        createEndpointByIdAndService("Athena", "com.amazonaws.eu-west-2.athena");
        createEndpointByIdAndService("S3", "com.amazonaws.eu-west-2.s3");
    }

    private void createEndpointByIdAndService(String id, String service) {
        InterfaceVpcEndpoint.Builder.create(this, id).vpc(vpc)
                .service(new InterfaceVpcEndpointService(service)).build();
    }

}

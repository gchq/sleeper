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
package sleeper.cdk.custom;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.CloudFormationCustomResourceEvent;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.Filter;
import software.amazon.awssdk.services.ec2.model.VpcEndpoint;

import java.util.List;
import java.util.Map;

public class VpcCheckLambda {
    private final Ec2Client vpcClient;

    public VpcCheckLambda() {
        this(Ec2Client.create());
    }

    public VpcCheckLambda(Ec2Client vpcClient) {
        this.vpcClient = vpcClient;
    }

    public void handleEvent(CloudFormationCustomResourceEvent event, Context context) {
        Map<String, Object> resourceProperties = event.getResourceProperties();
        String vpcId = (String) resourceProperties.get("vpcId");
        String region = (String) resourceProperties.get("region");

        switch (event.getRequestType()) {
            case "Create":
            case "Update":
                validateVpc(vpcId, region);
                break;
            case "Delete":
                break;
            default:
                throw new IllegalArgumentException("Invalid request type: " + event.getRequestType());
        }
    }

    private void validateVpc(String vpcId, String region) {
        List<VpcEndpoint> vpcEndpoints = vpcClient.describeVpcEndpoints(builder -> builder
                .filters(Filter.builder().name("vpc-id").values(vpcId).build(),
                        Filter.builder().name("service-name").values("com.amazonaws." + region + ".s3").build()))
                .vpcEndpoints();

        if (vpcEndpoints.size() != 1) {
            throw new IllegalArgumentException("The S3 endpoint for the requested VPC for this deployment is missing. This can mean very high cost "
                    + "for reading and writing data to your data buckets. We strongly encourage you to add an S3 endpoint to your"
                    + " VPC. To disable this check, set the instance property 'sleeper.vpc.endpoint.check' to false.");
        }
    }
}

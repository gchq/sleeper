/*
 * Copyright 2022-2024 Crown Copyright
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

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.DescribeVpcEndpointsRequest;
import com.amazonaws.services.ec2.model.DescribeVpcEndpointsResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.VpcEndpoint;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.CloudFormationCustomResourceEvent;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;

public class VpcCheckLambda {
    private final AmazonEC2 vpcClient;

    public VpcCheckLambda() {
        this(AmazonEC2ClientBuilder.defaultClient());
    }

    public VpcCheckLambda(AmazonEC2 vpcClient) {
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
        DescribeVpcEndpointsResult s3Endpoints = vpcClient.describeVpcEndpoints(new DescribeVpcEndpointsRequest()
                .withFilters(new Filter("vpc-id", Lists.newArrayList(vpcId)),
                        new Filter("service-name", Lists.newArrayList("com.amazonaws." + region + ".s3")))
        );
        List<VpcEndpoint> vpcEndpoints = s3Endpoints.getVpcEndpoints();

        if (vpcEndpoints.size() != 1) {
            throw new IllegalArgumentException("The S3 endpoint for the requested VPC for this deployment is missing. This can mean very high cost "
                    + "for reading and writing data to your data buckets. We strongly encourage you to add an S3 endpoint to your"
                    + " VPC. To disable this check, set the instance property 'sleeper.vpc.endpoint.check' to false.");
        }
    }
}

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
package sleeper.cdk.custom;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.mockito.Mockito;

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.DescribeVpcEndpointsRequest;
import com.amazonaws.services.ec2.model.DescribeVpcEndpointsResult;
import com.amazonaws.services.ec2.model.VpcEndpoint;
import com.amazonaws.services.lambda.runtime.events.CloudFormationCustomResourceEvent;

public class VpcCheckLambdaTest {

    @Test
    public void shouldThrowExceptionWhenVpcRequestReturnsNoMatchingEndpoints() {
        // Given
        AmazonEC2 mockEc2 = mock(AmazonEC2.class);
        VpcCheckLambda vpcCheckLambda = new VpcCheckLambda(mockEc2);

        // When
        when(mockEc2.describeVpcEndpoints(Mockito.any())).thenReturn(new DescribeVpcEndpointsResult());

        // Then
        try {
            vpcCheckLambda.handleEvent(CloudFormationCustomResourceEvent.builder()
                    .withRequestType("Create")
                    .withResourceProperties(new HashMap<>()).build(), null);
            fail("Exception expected");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage()).isNotNull();
        }
    }

    @Test
    public void shouldNotThrowExceptionWhenVpcRequestReturnsMatchingEndpoints() {
        // Given
        AmazonEC2 mockEc2 = mock(AmazonEC2.class);
        VpcCheckLambda vpcCheckLambda = new VpcCheckLambda(mockEc2);

        // When
        when(mockEc2.describeVpcEndpoints(Mockito.any())).thenReturn(new DescribeVpcEndpointsResult()
                .withVpcEndpoints(new VpcEndpoint()));

        // Then
        vpcCheckLambda.handleEvent(CloudFormationCustomResourceEvent.builder()
                .withRequestType("Create")
                .withResourceProperties(new HashMap<>()).build(), null);
        // no exceptions
    }

    @Test
    public void shouldPassVpcIdAndRegionFromThePropertiesToTheRequest() {
        // Given
        AmazonEC2 mockEc2 = mock(AmazonEC2.class);
        VpcCheckLambda vpcCheckLambda = new VpcCheckLambda(mockEc2);

        // When
        AtomicReference<DescribeVpcEndpointsRequest> requestReference = new AtomicReference<>();
        when(mockEc2.describeVpcEndpoints(Mockito.any()))
                .then((invocations) -> {
                    requestReference.set(invocations.getArgument(0, DescribeVpcEndpointsRequest.class));
                    return new DescribeVpcEndpointsResult()
                            .withVpcEndpoints(new VpcEndpoint());
                });

        Map<String, Object> properties = new HashMap<>();
        properties.put("vpcId", "myVpc");
        properties.put("region", "my-region-1");

        // Then
        vpcCheckLambda.handleEvent(CloudFormationCustomResourceEvent.builder()
                .withRequestType("Create")
                .withResourceProperties(properties).build(), null);

        DescribeVpcEndpointsRequest request = requestReference.get();
        assertThat(request.getFilters().get(0).getName()).isEqualTo("vpc-id");
        assertThat(request.getFilters().get(0).getValues().get(0)).isEqualTo("myVpc");
        assertThat(request.getFilters().get(1).getName()).isEqualTo("service-name");
        assertThat(request.getFilters().get(1).getValues().get(0)).isEqualTo("com.amazonaws.my-region-1.s3");
    }
}

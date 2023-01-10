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
package sleeper.cdk.custom;

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.DescribeVpcEndpointsRequest;
import com.amazonaws.services.ec2.model.DescribeVpcEndpointsResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.VpcEndpoint;
import com.amazonaws.services.lambda.runtime.events.CloudFormationCustomResourceEvent;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class VpcCheckLambdaTest {

    @Test
    public void shouldThrowExceptionWhenVpcRequestReturnsNoMatchingEndpoints() {
        // Given
        AmazonEC2 mockEc2 = mock(AmazonEC2.class);
        VpcCheckLambda vpcCheckLambda = new VpcCheckLambda(mockEc2);

        // When
        when(mockEc2.describeVpcEndpoints(Mockito.any())).thenReturn(new DescribeVpcEndpointsResult());

        // Then
        CloudFormationCustomResourceEvent event = CloudFormationCustomResourceEvent.builder()
                .withRequestType("Create")
                .withResourceProperties(new HashMap<>()).build();
        assertThatThrownBy(() -> vpcCheckLambda.handleEvent(event, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("endpoint");
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
        CloudFormationCustomResourceEvent event = CloudFormationCustomResourceEvent.builder()
                .withRequestType("Create")
                .withResourceProperties(new HashMap<>()).build();
        assertThatCode(() -> vpcCheckLambda.handleEvent(event, null))
                .doesNotThrowAnyException();
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

        assertThat(requestReference.get().getFilters())
                .extracting(Filter::getName, Filter::getValues).containsExactly(
                        tuple("vpc-id", Collections.singletonList("myVpc")),
                        tuple("service-name", Collections.singletonList("com.amazonaws.my-region-1.s3")));
    }
}

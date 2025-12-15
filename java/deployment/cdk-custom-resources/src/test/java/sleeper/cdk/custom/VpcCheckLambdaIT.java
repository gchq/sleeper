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

import com.amazonaws.services.lambda.runtime.events.CloudFormationCustomResourceEvent;
import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.ec2.Ec2Client;

import java.util.HashMap;
import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.localstack.test.WiremockAwsV2ClientHelper.wiremockAwsV2Client;

@WireMockTest
class VpcCheckLambdaIT {

    @Test
    void shouldThrowExceptionWhenVpcRequestReturnsNoMatchingEndpoints(WireMockRuntimeInfo runtimeInfo) {
        // Given
        stubFor(describeVpcEndpoints().willReturn(noVpcEndpointsResponse()));

        // When
        VpcCheckLambda vpcCheckLambda = new VpcCheckLambda(wiremockAwsV2Client(runtimeInfo, Ec2Client.builder()));
        CloudFormationCustomResourceEvent event = CloudFormationCustomResourceEvent.builder()
                .withRequestType("Create")
                .withResourceProperties(new HashMap<>()).build();

        // Then
        assertThatThrownBy(() -> vpcCheckLambda.handleEvent(event, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("endpoint");
    }

    @Test
    void shouldNotThrowExceptionWhenVpcRequestReturnsMatchingEndpoints(WireMockRuntimeInfo runtimeInfo) {
        // Given
        stubFor(describeVpcEndpoints().willReturn(singleVpcEndpointResponse()));

        // When
        VpcCheckLambda vpcCheckLambda = new VpcCheckLambda(wiremockAwsV2Client(runtimeInfo, Ec2Client.builder()));
        CloudFormationCustomResourceEvent event = CloudFormationCustomResourceEvent.builder()
                .withRequestType("Create")
                .withResourceProperties(new HashMap<>()).build();

        // Then
        assertThatCode(() -> vpcCheckLambda.handleEvent(event, null))
                .doesNotThrowAnyException();
    }

    @Test
    void shouldPassVpcIdAndRegionFromThePropertiesToTheRequest(WireMockRuntimeInfo runtimeInfo) {
        // Given
        stubFor(describeVpcEndpoints().willReturn(singleVpcEndpointResponse()));

        Map<String, Object> properties = new HashMap<>();
        properties.put("vpcId", "myVpc");
        properties.put("region", "my-region-1");

        // When
        VpcCheckLambda vpcCheckLambda = new VpcCheckLambda(wiremockAwsV2Client(runtimeInfo, Ec2Client.builder()));
        vpcCheckLambda.handleEvent(CloudFormationCustomResourceEvent.builder()
                .withRequestType("Create")
                .withResourceProperties(properties).build(), null);

        // Then
        verify(1, postRequestedFor(urlEqualTo("/"))
                .withRequestBody(containing("Filter.1.Name=vpc-id")
                        .and(containing("Filter.1.Value.1=myVpc"))
                        .and(containing("Filter.2.Name=service-name"))
                        .and(containing("Filter.2.Value.1=com.amazonaws.my-region-1.s3"))));
    }

    private static MappingBuilder describeVpcEndpoints() {
        return post("/").withRequestBody(containing("Action=DescribeVpcEndpoints"));
    }

    private static ResponseDefinitionBuilder noVpcEndpointsResponse() {
        return aResponse().withStatus(200)
                .withBody("<DescribeVpcEndpointsResponse><vpcEndpointSet></vpcEndpointSet></DescribeVpcEndpointsResponse>");
    }

    private static ResponseDefinitionBuilder singleVpcEndpointResponse() {
        return aResponse().withStatus(200)
                .withBody("<DescribeVpcEndpointsResponse><vpcEndpointSet>" +
                        "<item></item>" +
                        "</vpcEndpointSet></DescribeVpcEndpointsResponse>");
    }
}

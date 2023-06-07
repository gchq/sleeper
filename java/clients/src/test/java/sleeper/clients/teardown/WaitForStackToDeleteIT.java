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

package sleeper.clients.teardown;

import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.cloudformation.model.StackStatus;

import sleeper.clients.util.PollWithRetries;

import static com.amazonaws.services.s3.Headers.CONTENT_TYPE;
import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sleeper.clients.testutil.ClientWiremockTestHelper.wiremockCloudFormationClient;

@WireMockTest
class WaitForStackToDeleteIT {

    @Test
    void shouldFinishWaitingWhenStackIsDeleted(WireMockRuntimeInfo runtimeInfo) {
        // Given
        stubFor(describeStacksRequestWithStackName("test-stack")
                .willReturn(aResponseWithStackName("test-stack", StackStatus.DELETE_COMPLETE)));

        // When/Then
        assertThatCode(() -> waitForStacksToDelete(runtimeInfo, "test-stack"))
                .doesNotThrowAnyException();
    }

    @Test
    void shouldFinishWaitingWhenStackIsNotFound(WireMockRuntimeInfo runtimeInfo) {
        // See AWS documentation - it the stack is deleted it will not be found by its stack name:
        // https://docs.aws.amazon.com/AWSCloudFormation/latest/APIReference/API_DescribeStacks.html

        // Given
        stubFor(describeStacksRequestWithStackName("not-a-stack")
                .willReturn(stackDoesNotExistResponse("not-a-stack")));

        // When/Then
        assertThatCode(() -> waitForStacksToDelete(runtimeInfo, "not-a-stack"))
                .doesNotThrowAnyException();
    }

    @Test
    void shouldTimeOutWhenStackIsDeleting(WireMockRuntimeInfo runtimeInfo) {
        // Given
        stubFor(describeStacksRequestWithStackName("test-stack")
                .willReturn(aResponseWithStackName("test-stack", StackStatus.DELETE_IN_PROGRESS)));

        // When/Then
        assertThatThrownBy(() -> waitForStacksToDelete(runtimeInfo, "test-stack"))
                .isInstanceOf(PollWithRetries.TimedOutException.class);
    }

    private static void waitForStacksToDelete(WireMockRuntimeInfo runtimeInfo, String stackName) throws InterruptedException {
        WaitForStackToDelete.from(
                        PollWithRetries.intervalAndMaxPolls(0, 1),
                        wiremockCloudFormationClient(runtimeInfo), stackName)
                .pollUntilFinished();
    }

    private static MappingBuilder describeStacksRequestWithStackName(String stackName) {
        return post("/")
                .withHeader(CONTENT_TYPE, equalTo("application/x-www-form-urlencoded; charset=UTF-8"))
                .withRequestBody(containing("Action=DescribeStacks")
                        .and(containing("StackName=" + stackName)));
    }

    private static ResponseDefinitionBuilder aResponseWithStackName(String stackName, StackStatus stackStatus) {
        return aResponse().withStatus(200)
                .withBody("<DescribeStacksResponse xmlns=\"http://cloudformation.amazonaws.com/doc/2010-05-15/\"><DescribeStacksResult><Stacks><member>" +
                        "<StackName>" + stackName + "</StackName>" +
                        "<StackStatus>" + stackStatus + "</StackStatus>" +
                        "</member></Stacks></DescribeStacksResult></DescribeStacksResponse>");
    }

    private static ResponseDefinitionBuilder stackDoesNotExistResponse(String stackName) {
        return aResponse().withStatus(400)
                .withBody("<ErrorResponse xmlns=\"http://cloudformation.amazonaws.com/doc/2010-05-15/\"><Error>" +
                        "<Type>Sender</Type>" +
                        "<Code>ValidationError</Code>" +
                        "<Message>Stack with id " + stackName + " does not exist</Message>" +
                        "</Error></ErrorResponse>");
    }
}

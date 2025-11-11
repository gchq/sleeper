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
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static sleeper.cdk.custom.WiremockCweTestHelper.anyRequestedForCloudWatchEvents;
import static sleeper.cdk.custom.WiremockCweTestHelper.disableRuleRequest;
import static sleeper.cdk.custom.WiremockCweTestHelper.disableRuleRequestedFor;
import static sleeper.cdk.custom.WiremockCweTestHelper.wiremockCweClient;

@WireMockTest
public class PauseScheduledRuleLambdaIT {

    private PauseScheduledRuleLambda lambda;

    @BeforeEach
    void setUp(WireMockRuntimeInfo runtimeInfo) {
        lambda = lambda(runtimeInfo);
    }

    @DisplayName("Stop scheduled cloud watch rule")
    @Test
    void shouldShutdownCloudWatchRuleWhenSet() throws Exception {

        // Given
        String scheduledRuleName = "test-compaction-job-creation-rule";

        stubFor(disableRuleRequest()
                .willReturn(aResponse().withStatus(200)));

        // When
        lambda.handleEvent(eventHandlerForCloudWatch(scheduledRuleName, "Delete"), null);

        // Then
        verify(1, anyRequestedForCloudWatchEvents());
        verify(1, disableRuleRequestedFor(scheduledRuleName));

    }

    private CloudFormationCustomResourceEvent eventHandlerForCloudWatch(
            String scheduledRuleName, String event) {
        return CloudFormationCustomResourceEvent.builder()
                .withRequestType(event)
                .withResourceProperties(Map.of("scheduledRuleName", scheduledRuleName))
                .build();
    }

    private PauseScheduledRuleLambda lambda(WireMockRuntimeInfo runtimeInfo) {
        return new PauseScheduledRuleLambda(wiremockCweClient(runtimeInfo));
    }

}

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

import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.matching.RequestPatternBuilder;
import com.github.tomakehurst.wiremock.matching.StringValuePattern;
import software.amazon.awssdk.services.cloudwatchevents.CloudWatchEventsClient;

import static com.github.tomakehurst.wiremock.client.WireMock.anyRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.anyUrl;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.matching;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static sleeper.localstack.test.WiremockAwsV2ClientHelper.wiremockAwsV2Client;

/**
 * Helper methods to mock CloudWatch events.
 */
public class WiremockCweTestHelper {

    private WiremockCweTestHelper() {
    }

    public static final StringValuePattern MATCHING_DISABLE_RULE_OPERATION = matching("^AWSEvents\\.DisableRule$");
    public static final String OPERATION_HEADER = "X-Amz-Target";

    /**
     * Creates a mocked CloudwatchEvents client.
     *
     * @param  runtimeInfo wire mocks runtime info
     * @return             the CloudWatchEvents client
     */
    public static CloudWatchEventsClient wiremockCweClient(WireMockRuntimeInfo runtimeInfo) {
        return wiremockAwsV2Client(runtimeInfo, CloudWatchEventsClient.builder());
    }

    /**
     * Disable CloudWatch rule.
     *
     * @return a HTTP response
     */
    public static MappingBuilder disableRuleRequest() {
        return post("/")
                .withHeader(OPERATION_HEADER, MATCHING_DISABLE_RULE_OPERATION);
    }

    /**
     * Check for a disable rule request.
     *
     * @param  ruleName the rule name
     * @return          matching HTTP requests
     */
    public static RequestPatternBuilder disableRuleRequestedFor(String ruleName) {
        return postRequestedFor(urlEqualTo("/"))
                .withHeader(OPERATION_HEADER, MATCHING_DISABLE_RULE_OPERATION)
                .withRequestBody(matchingJsonPath("$.Name", equalTo(ruleName)));
    }

    /**
     * Check for any requests to the CloudWatchEvent client.
     *
     * @return matching HTTP requests
     */
    public static RequestPatternBuilder anyRequestedForCloudWatchEvents() {
        return anyRequestedFor(anyUrl())
                .withHeader(OPERATION_HEADER, matching("^AWSEvents\\..*"));
    }

}

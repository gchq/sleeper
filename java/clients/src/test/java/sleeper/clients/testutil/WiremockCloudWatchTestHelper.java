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

package sleeper.clients.testutil;

import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.matching.RequestPatternBuilder;
import com.github.tomakehurst.wiremock.matching.StringValuePattern;

import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.matching;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static sleeper.clients.testutil.ClientWiremockTestHelper.OPERATION_HEADER;

public class WiremockCloudWatchTestHelper {
    public static final StringValuePattern MATCHING_DISABLE_RULE_OPERATION = matching("^AWSEvents\\.DisableRule$");

    public static MappingBuilder disableRuleRequest() {
        return post("/")
                .withHeader(OPERATION_HEADER, MATCHING_DISABLE_RULE_OPERATION);
    }

    public static RequestPatternBuilder disableRuleRequestedFor(String ruleName) {
        return postRequestedFor(urlEqualTo("/"))
                .withHeader(OPERATION_HEADER, MATCHING_DISABLE_RULE_OPERATION)
                .withRequestBody(matchingJsonPath("$.Name", equalTo(ruleName)));
    }
}

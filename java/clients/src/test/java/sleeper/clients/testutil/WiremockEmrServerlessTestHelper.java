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

package sleeper.clients.testutil;

import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.matching.RequestPatternBuilder;
import com.github.tomakehurst.wiremock.matching.UrlPattern;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;

public class WiremockEmrServerlessTestHelper {

    private WiremockEmrServerlessTestHelper() {
    }

    public static MappingBuilder listActiveEmrApplicationsRequest() {
        return get(listRunningApplicationsUrl());
    }

    public static RequestPatternBuilder listActiveApplicationsRequested() {
        return getRequestedFor(listRunningApplicationsUrl());
    }

    public static ResponseDefinitionBuilder aResponseWithNoApplications() {
        return aResponse().withStatus(200)
                .withBody("{\"applications\": []}");
    }

    private static UrlPattern listRunningApplicationsUrl() {
        return urlEqualTo("/applications"
                + "?states=STARTING&states=STARTED&states=STOPPING");
    }

}

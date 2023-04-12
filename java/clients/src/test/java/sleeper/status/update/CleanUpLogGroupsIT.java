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
package sleeper.status.update;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.stream.Stream;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static sleeper.ClientWiremockTestHelper.wiremockCloudFormationClient;
import static sleeper.ClientWiremockTestHelper.wiremockLogsClient;

@WireMockTest
class CleanUpLogGroupsIT {

    @BeforeEach
    void setUp() {
        stubFor(post("/").willReturn(aResponse().withStatus(200)));
    }

    @Test
    void shouldDeleteAnEmptyLogGroupWhenNotInAStack(WireMockRuntimeInfo runtimeInfo) {
        // Given

        // When
        streamLogGroupNamesToDelete(runtimeInfo);
    }

    private static Stream<String> streamLogGroupNamesToDelete(WireMockRuntimeInfo runtimeInfo) {
        return CleanUpLogGroups.streamLogGroupNamesToDelete(
                wiremockLogsClient(runtimeInfo),
                wiremockCloudFormationClient(runtimeInfo));
    }
}

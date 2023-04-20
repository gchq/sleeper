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

package sleeper.clients.status.report.ingest.job;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import org.junit.jupiter.api.Test;

import sleeper.configuration.properties.InstanceProperties;

import java.util.Map;

import static sleeper.clients.testutil.ClientWiremockTestHelper.wiremockEmrClient;
import static sleeper.configuration.properties.InstancePropertiesTestHelper.createTestInstanceProperties;

@WireMockTest
public class PersistentEMRStepCountIT {
    private final InstanceProperties properties = createTestInstanceProperties();

    @Test
    void shouldFindNoStepsForCluster() {
        // TODO
    }

    private Map<String, Integer> getStepCount(WireMockRuntimeInfo runtimeInfo) {
        return PersistentEMRStepCount.from(wiremockEmrClient(runtimeInfo));
    }
}

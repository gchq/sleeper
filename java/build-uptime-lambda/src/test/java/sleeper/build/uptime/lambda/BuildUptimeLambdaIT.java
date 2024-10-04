/*
 * Copyright 2022-2024 Crown Copyright
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
package sleeper.build.uptime.lambda;

import com.amazonaws.services.lambda.runtime.events.ScheduledEvent;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import org.junit.jupiter.api.Test;

import static sleeper.build.uptime.lambda.WiremockTestHelper.wiremockEc2Client;

@WireMockTest
public class BuildUptimeLambdaIT {

    @Test
    void shouldStartEc2(WireMockRuntimeInfo runtimeInfo) {
        BuildUptimeLambda lambda = new BuildUptimeLambda(wiremockEc2Client(runtimeInfo));
        lambda.handleRequest(new ScheduledEvent(), null);
    }

}

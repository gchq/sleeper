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
package sleeper.clients.deploy;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.core.deploy.SleeperScheduleRule;
import sleeper.core.properties.instance.InstanceProperties;

import java.util.List;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static sleeper.clients.testutil.ClientWiremockTestHelper.wiremockCloudWatchClient;
import static sleeper.clients.testutil.WiremockCloudWatchTestHelper.anyRequestedForCloudWatchEvents;
import static sleeper.clients.testutil.WiremockCloudWatchTestHelper.disableRuleRequest;
import static sleeper.clients.testutil.WiremockCloudWatchTestHelper.disableRuleRequestedFor;
import static sleeper.clients.testutil.WiremockCloudWatchTestHelper.enableRuleRequest;
import static sleeper.clients.testutil.WiremockCloudWatchTestHelper.enableRuleRequestedFor;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.COMPACTION_JOB_CREATION_CLOUDWATCH_RULE;
import static sleeper.core.properties.instance.CdkDefinedInstanceProperty.GARBAGE_COLLECTOR_CLOUDWATCH_RULE;
import static sleeper.core.properties.testutils.InstancePropertiesTestHelper.createTestInstanceProperties;

@WireMockTest
public class AwsScheduleRulesIT {

    private final InstanceProperties instanceProperties = createTestInstanceProperties();
    private final List<SleeperScheduleRule> rulesList = List.of(
            SleeperScheduleRule.COMPACTION_JOB_CREATION, SleeperScheduleRule.GARBAGE_COLLECTOR);

    AwsScheduleRules rules;

    @BeforeEach
    void setUp(WireMockRuntimeInfo runtimeInfo) {
        rules = new AwsScheduleRules(wiremockCloudWatchClient(runtimeInfo), rulesList);
    }

    @Test
    void shouldPauseInstanceByProperties() throws Exception {
        // Given
        instanceProperties.set(COMPACTION_JOB_CREATION_CLOUDWATCH_RULE, "test-compaction-job-creation-rule");
        instanceProperties.set(GARBAGE_COLLECTOR_CLOUDWATCH_RULE, "test-garbage-collector-rule");
        stubFor(disableRuleRequest()
                .willReturn(aResponse().withStatus(200)));

        // When
        rules.pauseInstance(instanceProperties);

        // Then
        verify(2, anyRequestedForCloudWatchEvents());
        verify(1, disableRuleRequestedFor("test-compaction-job-creation-rule"));
        verify(1, disableRuleRequestedFor("test-garbage-collector-rule"));
    }

    @Test
    void shouldPauseInstanceById() {
        // Given
        stubFor(disableRuleRequest()
                .willReturn(aResponse().withStatus(200)));

        // When
        rules.pauseInstance("my-instance");

        // Then
        verify(2, anyRequestedForCloudWatchEvents());
        verify(1, disableRuleRequestedFor("my-instance-CompactionJobCreationRule"));
        verify(1, disableRuleRequestedFor("my-instance-GarbageCollectorPeriodicTrigger"));
    }

    @Test
    void shouldStartInstanceByProperties() {
        // Given
        instanceProperties.set(COMPACTION_JOB_CREATION_CLOUDWATCH_RULE, "test-compaction-job-creation-rule");
        instanceProperties.set(GARBAGE_COLLECTOR_CLOUDWATCH_RULE, "test-garbage-collector-rule");
        stubFor(enableRuleRequest()
                .willReturn(aResponse().withStatus(200)));

        // When
        rules.startInstance(instanceProperties);

        // Then
        verify(2, anyRequestedForCloudWatchEvents());
        verify(1, enableRuleRequestedFor("test-compaction-job-creation-rule"));
        verify(1, enableRuleRequestedFor("test-garbage-collector-rule"));
    }

    @Test
    void shouldStartInstanceById() {
        // Given
        stubFor(enableRuleRequest()
                .willReturn(aResponse().withStatus(200)));

        // When
        rules.startInstance("my-instance");

        // Then
        verify(2, anyRequestedForCloudWatchEvents());
        verify(1, enableRuleRequestedFor("my-instance-CompactionJobCreationRule"));
        verify(1, enableRuleRequestedFor("my-instance-GarbageCollectorPeriodicTrigger"));
    }

}

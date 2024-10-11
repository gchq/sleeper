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
package sleeper.systemtest.suite;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import sleeper.core.properties.validation.OptionalStack;
import sleeper.systemtest.dsl.SleeperSystemTest;
import sleeper.systemtest.suite.testutil.Slow;
import sleeper.systemtest.suite.testutil.SystemTest;

import java.util.LinkedHashSet;
import java.util.Set;

import static sleeper.systemtest.suite.fixtures.SystemTestInstance.REENABLE_OPTIONAL_STACKS;

@SystemTest
// Slow because it needs to do many CDK deployments
@Slow
public class RedeployOptionalStacksST {

    private static final Set<OptionalStack> REDEPLOYABLE_STACKS = new LinkedHashSet<>(OptionalStack.all());
    static {
        // We're currently unable to configure log groups related to an EKS cluster,
        // so it fails to redeploy because those log groups already exist.
        REDEPLOYABLE_STACKS.remove(OptionalStack.EksBulkImportStack);
    }

    @BeforeEach
    void setUp(SleeperSystemTest sleeper) {
        sleeper.connectToInstance(REENABLE_OPTIONAL_STACKS);
    }

    @AfterEach
    void tearDown(SleeperSystemTest sleeper) {
        sleeper.disableOptionalStacks(OptionalStack.all());
    }

    @Test
    void shouldDisableAndReenableAllOptionalStacks(SleeperSystemTest sleeper) {
        sleeper.enableOptionalStacks(REDEPLOYABLE_STACKS);
        sleeper.disableOptionalStacks(OptionalStack.all());
        sleeper.enableOptionalStacks(REDEPLOYABLE_STACKS);
    }

}

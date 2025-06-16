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
package sleeper.compaction.job.creation.lambda;

import org.junit.jupiter.api.Test;

import sleeper.core.deploy.LambdaHandler;

import static org.assertj.core.api.Assertions.assertThat;

public class CompactionJobCreationLambdaHandlerTest {

    @Test
    void shouldMatchTriggerLambdaHandlerDeclaration() {
        assertThat(LambdaHandler.COMPACTION_JOB_CREATOR_TRIGGER.getHandler())
                .isEqualTo(CreateCompactionJobsTriggerLambda.class.getName() + "::handleRequest");
    }

    @Test
    void shouldMatchCreationLambdaHandlerDeclaration() {
        assertThat(LambdaHandler.COMPACTION_JOB_CREATOR.getHandler())
                .isEqualTo(CreateCompactionJobsLambda.class.getName() + "::handleRequest");
    }

    @Test
    void shouldMatchDispatchLambdaHandlerDeclaration() {
        assertThat(LambdaHandler.COMPACTION_JOB_DISPATCHER.getHandler())
                .isEqualTo(CompactionJobDispatchLambda.class.getName() + "::handleRequest");
    }

}

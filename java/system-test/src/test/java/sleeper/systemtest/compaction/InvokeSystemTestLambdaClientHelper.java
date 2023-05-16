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

package sleeper.systemtest.compaction;

import sleeper.systemtest.util.InvokeSystemTestLambda;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.COMPACTION_JOB_CREATION_LAMBDA_FUNCTION;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_TASK_CREATION_LAMBDA_FUNCTION;

public class InvokeSystemTestLambdaClientHelper {
    private Runnable compactionJobLambda;
    private Runnable compactionTaskLambda;

    public static InvokeSystemTestLambdaClientHelper clientBuilder() {
        return new InvokeSystemTestLambdaClientHelper();
    }

    public InvokeSystemTestLambdaClientHelper compactionJobLambda(Runnable compactionJobLambda) {
        this.compactionJobLambda = compactionJobLambda;
        return this;
    }

    public InvokeSystemTestLambdaClientHelper compactionTaskLambda(Runnable compactionTaskLambda) {
        this.compactionTaskLambda = compactionTaskLambda;
        return this;
    }

    public InvokeSystemTestLambda.Client build() {
        return lambdaFunctionProperty -> {
            if (lambdaFunctionProperty.equals(COMPACTION_JOB_CREATION_LAMBDA_FUNCTION)) {
                compactionJobLambda.run();
            } else if (lambdaFunctionProperty.equals(SPLITTING_COMPACTION_TASK_CREATION_LAMBDA_FUNCTION)) {
                compactionTaskLambda.run();
            }
        };
    }

}

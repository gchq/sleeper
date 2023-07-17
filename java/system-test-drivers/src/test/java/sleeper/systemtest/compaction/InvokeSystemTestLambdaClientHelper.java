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

import sleeper.configuration.properties.InstanceProperty;
import sleeper.systemtest.drivers.util.InvokeSystemTestLambda;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static sleeper.configuration.properties.SystemDefinedInstanceProperty.COMPACTION_JOB_CREATION_LAMBDA_FUNCTION;
import static sleeper.configuration.properties.SystemDefinedInstanceProperty.SPLITTING_COMPACTION_TASK_CREATION_LAMBDA_FUNCTION;

public class InvokeSystemTestLambdaClientHelper {
    private final Map<String, Runnable> lambdaByPropertyName = new HashMap<>();

    public static InvokeSystemTestLambdaClientHelper lambdaClientBuilder() {
        return new InvokeSystemTestLambdaClientHelper();
    }

    public InvokeSystemTestLambdaClientHelper compactionJobCreation(Runnable compactionJobLambda) {
        return lambda(COMPACTION_JOB_CREATION_LAMBDA_FUNCTION, compactionJobLambda);
    }

    public InvokeSystemTestLambdaClientHelper splittingCompactionTaskCreation(Runnable compactionTaskLambda) {
        return lambda(SPLITTING_COMPACTION_TASK_CREATION_LAMBDA_FUNCTION, compactionTaskLambda);
    }

    public InvokeSystemTestLambdaClientHelper lambda(InstanceProperty property, Runnable behaviour) {
        lambdaByPropertyName.put(property.getPropertyName(), behaviour);
        return this;
    }

    public InvokeSystemTestLambda.Client build() {
        return lambdaFunctionProperty ->
                Optional.ofNullable(lambdaByPropertyName.get(lambdaFunctionProperty.getPropertyName()))
                        .ifPresent(Runnable::run);
    }

}
